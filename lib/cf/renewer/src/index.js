'use strict';

const _ = require('underscore');
const extend = _.extend;
const map = _.map;
const memoize = _.memoize;
const pairs = _.pairs;
const pick = _.pick;

const util = require('util');

const batch = require('abacus-batch');
const breaker = require('abacus-breaker');
const cluster = require('abacus-cluster');
const dbclient = require('abacus-dbclient');
const moment = require('moment');
const oauth = require('abacus-oauth');
const partition = require('abacus-partition');
const perf = require('abacus-perf');
const request = require('abacus-request');
const retry = require('abacus-retry');
const router = require('abacus-router');
const throttle = require('abacus-throttle');
const urienv = require('abacus-urienv');
const usageSchemas = require('abacus-usage-schemas');
const webapp = require('abacus-webapp');
const yieldable = require('abacus-yieldable');

const throttleLimit = process.env.THROTTLE ? parseInt(process.env.THROTTLE) :
  100;
const batchSize = process.env.BATCH_SIZE ? parseInt(process.env.BATCH_SIZE) :
  100;

// if a batch is throttled, then throttle limits the number of calls made to
// the batch function limiting the number of batches. In order to avoid that
// all the batch functions when throttled should have a throttle value that is
// multiplied by the batch.
const reliableRequest = throttle(retry(breaker(batch(request))),
  batchSize * throttleLimit);

// Setup debug log
const debug = require('abacus-debug')('abacus-cf-renewer');
const edebug = require('abacus-debug')('e-abacus-cf-renewer');

// Create an express router
const routes = router();

// Resolve service URIs
const uris = memoize(() => urienv({
  auth_server : 9882,
  collector   : 9080,
  db          : 5984,
  provisioning: 9880
}));

// Retry config
const oneDayInMilliseconds = 86400000;
const retryConfig = {
  retryInterval : parseInt(process.env.RETRY_INTERVAL) || oneDayInMilliseconds
};

// Function call statistics
const statistics = {
  db: {
    readSuccess: 0,
    readFailure: 0,
    docsRead: 0
  },
  usage: {
    missingToken: 0,
    getFailures: 0,
    getSuccess: 0,
    reportFailures: 0,
    reportSuccess: 0,
    reportConflict: 0
  },
  plan: {
    getFailures: 0,
    getSuccess: 0
  },
  retries: {
    count: 0
  }
};

const errors = {
  missingToken: false,
  noGetEverHappened: true,
  noReportEverHappened: true,
  consecutiveGetFailures: 0,
  consecutiveReportFailures: 0,
  lastError: '',
  lastErrorTimestamp: ''
};

const registerError = (message, error, response, name, time) => {
  const errorMessage = error || response ?
    util.format('%s; error: %j; response: %j', message, error, response) :
    message;

  errors.lastError = errorMessage;
  errors.lastErrorTimestamp = new Date().toISOString();

  edebug(errorMessage);

  if (name && time)
    perf.report(name, time, undefined, new Error(errorMessage));
};

// Default page size
// Each page is processed in parallel, so by default we limit the size to the
// number of allowed connections to collector
const pageSize = parseInt(process.env.PAGE_SIZE) || throttleLimit;

// Use secure routes or not
const secured = process.env.SECURED === 'true';

// Abacus system token
const systemToken = secured ? oauth.cache(uris().auth_server,
  process.env.ABACUS_CLIENT_ID, process.env.ABACUS_CLIENT_SECRET,
  'abacus.usage.write abacus.usage.read') :
  undefined;

// Partitioning function that can be used for range queries
const checkKeyPart = partition.partitioner(partition.bucket,
  partition.period, partition.forward, partition.balance, true);

// DB for storing the last processed app and app-usage GUIDs
const carryOverDB = yieldable(throttle(retry(breaker(batch(
  dbclient(checkKeyPart, dbclient.dburi(uris().db, 'abacus-carry-over'))
)))));

const authHeader = (token) => token ? { authorization: token() } : {};

// The scaling factor of each time window for creating the date string
// [Second, Minute, Hour, Day, Month]
const slack = /^[0-9]+[MDhms]$/.test(process.env.SLACK) ? {
  scale : process.env.SLACK.charAt(process.env.SLACK.length - 1),
  width : process.env.SLACK.match(/[0-9]+/)[0]
} : {
  scale : 'm',
  width : 10
};

// Millisecond representation of the time dimensions
const msDimensions = {
  M: 2678400000,
  D: 86400000,
  h: 3600000,
  m: 60000,
  s: 1000
};

const slackLimit = msDimensions[slack.scale] * slack.width;

const getUsageDoc = (id, systemToken, cb) => {
  debug('Getting usage doc with id %s from collector ...', id);

  const t0 = Date.now();
  reliableRequest.get(':collector/v1/metering/collected/usage/:usage_id', {
    collector: uris().collector,
    usage_id: id,
    headers: authHeader(systemToken)
  }, (error, response) => {
    const doc = response ? response.body : undefined;

    if (!error && response && response.statusCode === 200) {
      debug('Got usage %j with headers %j', doc, response.headers);
      statistics.usage.getSuccess++;
      errors.consecutiveGetFailures = 0;
      errors.noGetEverHappened = false;
      perf.report('get', t0);
      cb(doc);
      return;
    }

    statistics.usage.getFailures++;
    errors.consecutiveGetFailures++;
    registerError(util.format('Failed getting usage: %j', doc),
      error, response, 'get', t0);
    cb(undefined, error, response);
  });
};

const zeroUsage = (usage) => {
  const clonedUsage = extend({}, usage);

  for(let measureObject of clonedUsage.measured_usage)
    if (measureObject.measure.startsWith('previous'))
      measureObject.quantity = 0;

  return clonedUsage;
};

const refreshUsage = (usage) => {
  const monthStart = moment().utc().startOf('month').valueOf();
  usage.start = monthStart;
  usage.end = monthStart;
  return zeroUsage(usage);
};

const extractUsageDocs = (usageDocs) =>
  map(usageDocs, (usageDoc) => usageDoc.doc);

const resourceUsageSchemaProperties = map(
  pairs(usageSchemas.resourceUsage.json().properties), (p) => p[0]);

const sanitizeUsageDoc = (doc) => pick(doc,
  resourceUsageSchemaProperties);

const reportUsage = (doc, token, cb) => {
  debug('Reporting usage with id %s ...', doc.id);

  const t0 = Date.now();
  const usage = sanitizeUsageDoc(refreshUsage(doc));

  reliableRequest.post(':collector/v1/metering/collected/usage', {
    collector: uris().collector,
    headers: authHeader(token),
    body: usage
  }, (error, response) => {
    if (!error && response) {
      if (response.statusCode === 201) {
        debug('Successfully reported usage %j with headers %j',
          usage, response.headers);
        statistics.usage.reportSuccess++;
        errors.consecutiveReportFailures = 0;
        errors.noReportEverHappened = false;
        perf.report('report', t0);
        cb();
        return;
      }
      if (response.statusCode === 409) {
        debug('Conflicting usage %j. Response: %j', usage, response);
        statistics.usage.reportConflict++;
        errors.noReportEverHappened = false;
        perf.report('report', t0, undefined, undefined, undefined, 'rejected');
        cb();
        return;
      }
    }
    statistics.usage.reportFailures++;
    errors.consecutiveReportFailures++;
    registerError(util.format('Failed reporting usage: %j', usage),
      error, response, 'report', t0);
    cb(error, response);
  });
};

const setRenewTimeout = (fn , interval) => {
  clearTimeout(module.usageRenewer);
  module.usageRenewer = setTimeout(fn, interval);
  debug('Retry interval set to %d ms after %d retries',
    interval, statistics.retries.count);
  statistics.retries.count++;
};

const buildError = (op, doc, error, response) => ({
  op: op,
  doc: doc,
  error: error,
  response: response
});

const renewUsagePage = (systemToken, startId, endId, skip,
  { failure, success }) => {
  debug('Reading usage data from %s to %s with limit %d',
    startId, endId, pageSize);

  const t0 = Date.now();

  yieldable.functioncb(function *() {
    // Fetch all docs from the previous month
    const dbDocs = yield carryOverDB.allDocs({
      include_docs: true,
      startkey: startId,
      endkey: endId,
      limit: pageSize,
      skip: skip
    });
    debug('Found %d DB documents ...', dbDocs.rows.length);
    statistics.db.docsRead += dbDocs.rows.length;
    statistics.db.readSuccess++;
    perf.report('db', t0);

    return dbDocs.rows;
  })((error, usage) => {
    if (error) {
      statistics.db.readFailure++;
      registerError(util.format('Failed reading usage data from %s to %s ' +
          'with limit %d', startId, endId, pageSize),
        error, undefined, 'db', t0);
      failure(error, usage);
    }

    const references = extractUsageDocs(usage);

    if (usage.length === 0) {
      debug('Processed all DB documents.');
      success();
      return;
    }

    debug('Processing %d DB documents ...', usage.length);
    let numDocs = 0;
    const countReportedDocs = () => {
      numDocs++;
      if (numDocs === usage.length) {
        if (usage.length < pageSize) {
          debug('Processed the last %d DB documents.', usage.length);
          success();
          return;
        }

        renewUsagePage(systemToken, startId, endId, skip + numDocs,
          { failure, success });
      }
    };

    for (let ref of references) {
      const id = ref.collector_id;
      getUsageDoc(id, systemToken, (doc, error, response) => {
        if (error || response) {
          failure(buildError('get', { collector_id: id }, error, response),
            response);
          return;
        }

        reportUsage(doc, systemToken, (error, response) => {
          if (error || response) {
            failure(buildError('report', doc, error, response),
              response);
            return;
          }

          countReportedDocs();
        });
      });
    }
  });
};

const renewUsage = (systemToken, { failure, success }) => {
  debug('Usage renewer started ...');

  if (secured && !systemToken()) {
    setRenewTimeout(() => renewUsage(systemToken, { failure, success }),
      5000);
    statistics.usage.missingToken++;
    errors.missingToken = true;
    registerError('Missing token');
    failure('Missing token');
    return;
  }

  debug('Scheduling next execution on %s',
    moment().add(retryConfig.retryInterval, 'milliseconds').toDate());
  setRenewTimeout(() => renewUsage(systemToken, { failure, success }),
    retryConfig.retryInterval);

  // Calculate previous month boundaries
  const startTime =
    moment().utc().subtract(1, 'months').startOf('month').
    subtract(slackLimit, 'milliseconds');
  const endTime =
    moment().utc().subtract(1, 'months').endOf('month');
  debug('Will scan for usage between %s and %s with slack window %d ms',
    startTime.toISOString(), endTime.toISOString(), slackLimit);

  // Compute the query range
  const startId = 't/' + dbclient.pad16(startTime.valueOf());
  const endId = 't/' + dbclient.pad16(endTime.valueOf());
  debug('Searching for docs in the range [%s, %s)', startId, endId);

  renewUsagePage(systemToken, startId, endId, 0, {
    failure: (error, response) => {
      edebug('Failed to renew usage error: %o, response: %j', error, response);
      failure(error, response || error && error.response || undefined);
    },
    success: () => success()
  });
};

const stopRenewer = (cb = () => {}) => {
  edebug('Cancelling timers');
  clearTimeout(module.usageRenewer);

  if (typeof cb === 'function')
    cb();
};

const startRenewer = () => {
  debug('Starting renewer ...');

  // Start token functions
  if (secured)
    systemToken.start();

  setRenewTimeout(() => renewUsage(systemToken, {
    success: () => {
      debug('Renewer completed successfully');
    },
    failure: () => {
      edebug('Renewer failed');
    }
  }), 0);

  // Cancel scheduled timers
  process.on('exit', stopRenewer);
};

routes.get('/v1/cf/renewer', throttle(function *(req) {
  return {
    body: {
      renewer: {
        performance: {
          db: perf.stats('db'),
          get: perf.stats('get'),
          report: perf.stats('report'),
          plan: perf.stats('plan')
        },
        statistics: statistics,
        errors: errors
      }
    }
  };
}));

// Create a CF renew app
const renew = () => {
  debug('Starting renewer app ...');
  cluster.singleton();

  if (cluster.isWorker()) {
    debug('Starting renewer worker');
    startRenewer();
  }

  // Create the Webapp
  const app = webapp();

  if(secured)
    app.use(routes, oauth.validator(process.env.JWTKEY, process.env.JWTALGO));
  else
    app.use(routes);

  return app;
};

// Command line interface, create the renew app and listen
const runCLI = () => renew().listen();

// Export our public functions
module.exports = renew;
module.exports.retryConfig = retryConfig;
module.exports.sanitizeUsageDoc = sanitizeUsageDoc;
module.exports.statistics = statistics;
module.exports.errors = errors;
module.exports.stopRenewer = stopRenewer;
module.exports.renewUsage = renewUsage;
module.exports.zeroUsage = zeroUsage;
module.exports.runCLI = runCLI;
