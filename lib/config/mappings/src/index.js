'use strict';

const fs = require('fs');
const path = require('path');

const _ = require('underscore');
const extend = _.extend;

const batch = require('abacus-batch');
const breaker = require('abacus-breaker');
const dbclient = require('abacus-dbclient');
const lockcb = require('abacus-lock');
const lru = require('abacus-lrucache');
const partition = require('abacus-partition');
const retry = require('abacus-retry');
const urienv = require('abacus-urienv');
const yieldable = require('abacus-yieldable');

// Setup debug log
const debug = require('abacus-debug')('abacus-plan-mappings');

// Cache locks
const meteringMappingsLock = yieldable(lockcb);
const ratingMappingsLock = yieldable(lockcb);
const pricingMappingsLock = yieldable(lockcb);

const uris = urienv({
  db: 5984
});

// Configure metering plan mappings db
const meteringMappingsDb = yieldable(retry(breaker(batch(
  dbclient(partition.singleton,
    dbclient.dburi(uris.db, 'abacus-metering-plan-mappings'))))));

// Configure rating plan mappings db
const ratingMappingsDb = yieldable(retry(breaker(batch(
  dbclient(partition.singleton,
    dbclient.dburi(uris.db, 'abacus-rating-plan-mappings'))))));

// Configure pricing plan mappings db
const pricingMappingsDb = yieldable(retry(breaker(batch(
  dbclient(partition.singleton,
    dbclient.dburi(uris.db, 'abacus-pricing-plan-mappings'))))));

// Default maps from (resource_type, plan_name) to plan_id
const mappings = path.join(__dirname, 'plans');
const defaultMeteringMapping = JSON.parse(
  fs.readFileSync(path.join(mappings, 'metering.json')));
const defaultPricingMapping = JSON.parse(
  fs.readFileSync(path.join(mappings, 'pricing.json')));
const defaultRatingMapping = JSON.parse(
  fs.readFileSync(path.join(mappings, 'rating.json')));

// Maintain a cache of metering mappings
const meteringMappingsCache = lru({
  max: 1000,
  maxAge: 1000 * 60 * 20
});

// Cache a metering mapping
const meteringMappingCache = (key, planId) => {
  meteringMappingsCache.set(key, planId);
  return planId;
};

// Return a metering plan from the cache
const meteringMappingCached = (key) => {
  return meteringMappingsCache.get(key);
};

// Maintain a cache of rating mappings
const ratingMappingsCache = lru({
  max: 1000,
  maxAge: 1000 * 60 * 20
});

// Cache a rating mapping
const ratingMappingCache = (key, planId) => {
  ratingMappingsCache.set(key, planId);
  return planId;
};

// Return a rating plan from the cache
const ratingMappingCached = (key) => {
  return ratingMappingsCache.get(key);
};

// Maintain a cache of pricing mappings
const pricingMappingsCache = lru({
  max: 1000,
  maxAge: 1000 * 60 * 20
});

// Cache a pricing mapping
const pricingMappingCache = (key, planId) => {
  pricingMappingsCache.set(key, planId);
  return planId;
};

// Return a pricing plan from the cache
const pricingMappingCached = (key) => {
  return pricingMappingsCache.get(key);
};

// Create metering mapping (resourceType, planName) to planId
const newMeteringMapping = function *(resourceType, planName, planId) {
  debug('Storing new metering mapping (%s, %s) -> %s',
    resourceType, planName, planId);
  const id = ['k', resourceType, planName].join('/');
  yield meteringMappingsDb.put(extend({}, { planId: planId }, {
    _id: id
  }));
};

// Create rating mapping (resourceType, planName) to planId
const newRatingMapping = function *(resourceType, planName, planId) {
  debug('Storing new rating mapping (%s, %s) -> %s',
    resourceType, planName, planId);
  const id = ['k', resourceType, planName].join('/');
  yield ratingMappingsDb.put(extend({}, { planId: planId }, {
    _id: id
  }));
};

// Create pricing mapping (resourceType, planName) to planId
const newPricingMapping = function *(resourceType, planName, planId) {
  debug('Storing new pricing mapping (%s, %s) -> %s',
    resourceType, planName, planId);
  const id = ['k', resourceType, planName].join('/');
  yield pricingMappingsDb.put(extend({}, { planId: planId }, {
    _id: id
  }));
};

// Retrieve a mapped metering plan. Search in cache, local resources and
// finally in the metering plan mappings database
const mappedMeteringPlan = function *(resourceType, planName) {
  const id = ['k', resourceType, planName].join('/');
  const unlock = yield meteringMappingsLock(id);
  try {
    debug('Retrieving metering plan for (%s, %s)', resourceType, planName);

    // Look in our cache first
    const cachedPlanId = meteringMappingCached(id);
    if(cachedPlanId) {
      debug('Metering plan %s found in cache', cachedPlanId);
      return cachedPlanId;
    }

    // Look in the metering plan mappings db
    const doc = yield meteringMappingsDb.get(id);
    if (doc)
      debug('Metering plan %s found in db', id);
    return doc ? meteringMappingCache(id, dbclient.undbify(doc).planId) : doc;
  }
  finally {
    unlock();
  }
};

// Retrieve a mapped rating plan. Search in cache, then in the
// rating plan mappings database
const mappedRatingPlan = function *(resourceType, planName) {
  const id = ['k', resourceType, planName].join('/');
  const unlock = yield ratingMappingsLock(id);
  try {
    debug('Retrieving rating plan for (%s, %s)', resourceType, planName);

    // Look in our cache first
    const cachedPlanId = ratingMappingCached(id);
    if(cachedPlanId) {
      debug('Rating plan %s found in cache', id);
      return cachedPlanId;
    }

    // Look in the metering plan mappings db
    const doc = yield ratingMappingsDb.get(id);
    if (doc)
      debug('Rating plan %s found in db', cachedPlanId);
    return doc ? ratingMappingCache(id, dbclient.undbify(doc).planId) : doc;
  }
  finally {
    unlock();
  }
};

// Retrieve a mapped pricing plan. Search in cache, then in the
// pricing plan mappings database
const mappedPricingPlan = function *(resourceType, planName) {
  const id = ['k', resourceType, planName].join('/');
  const unlock = yield pricingMappingsLock(id);
  try {
    debug('Retrieving pricing plan for (%s, %s)', resourceType, planName);

    // Look in our cache first
    const cachedPlanId = pricingMappingCached(id);
    if(cachedPlanId) {
      debug('Pricing plan %s found in cache', cachedPlanId);
      return cachedPlanId;
    }

    // Look in the metering plan mappings db
    const doc = yield pricingMappingsDb.get(id);
    if (doc)
      debug('Pricing plan %s found in db', id);
    return doc ? pricingMappingCache(id, dbclient.undbify(doc).planId) : doc;
  }
  finally {
    unlock();
  }
};

const storeMapping = (type, db, readFn, createFn, mapping, cb) => {
  debug('Creating %s plan mappings ...', type);
  yieldable.functioncb(function *() {
    for(let resourceType in mapping)
      for (let planName in mapping[resourceType])
        if (! (yield readFn(resourceType, planName)))
          yield createFn(resourceType, planName,
            mapping[resourceType][planName]);
  })((error) => {
    if(error) {
      debug('Failed to store %s default mappings: %o', type, error);
      throw new Error('Failed to store default mappings');
    }
    debug('Default %s plan mappings created', type);
    cb();
  });
};

// Populate mapping dbs and cache with the default mappings
const storeDefaultMappings = (cb) => {
  let callCount = 0;
  const countCb = () => {
    if(++callCount == 3)
      cb();
  };

  storeMapping('metering', meteringMappingsDb, mappedMeteringPlan,
    newMeteringMapping, defaultMeteringMapping, countCb);
  storeMapping('rating', ratingMappingsDb, mappedRatingPlan,
    newRatingMapping, defaultRatingMapping, countCb);
  storeMapping('pricing', pricingMappingsDb, mappedPricingPlan,
    newPricingMapping, defaultPricingMapping, countCb);
};

// Module exports
module.exports.sampleMetering = defaultMeteringMapping;
module.exports.samplePricing = defaultPricingMapping;
module.exports.sampleRating = defaultRatingMapping;

module.exports.newMeteringMapping = newMeteringMapping;
module.exports.newRatingMapping = newRatingMapping;
module.exports.newPricingMapping = newPricingMapping;

module.exports.mappedMeteringPlan = mappedMeteringPlan;
module.exports.mappedRatingPlan = mappedRatingPlan;
module.exports.mappedPricingPlan = mappedPricingPlan;

module.exports.storeDefaultMappings = storeDefaultMappings;
