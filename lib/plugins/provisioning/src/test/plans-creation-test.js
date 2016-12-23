'use strict';

// Minimal example implementation of an Abacus provisioning plugin.

const _ = require('underscore');
const request = require('abacus-request');
const cluster = require('abacus-cluster');
const oauth = require('abacus-oauth');
const dbclient = require('abacus-dbclient');

const extend = _.extend;
const omit = _.omit;

// Configure test db URL prefix
process.env.DB = process.env.DB || 'test';

// Mock the cluster module
require.cache[require.resolve('abacus-cluster')].exports =
  extend((app) => app, cluster);

// Mock the oauth module with a spy
const oauthspy = spy((req, res, next) => next());
const oauthmock = extend({}, oauth, {
  validator: () => oauthspy
});
require.cache[require.resolve('abacus-oauth')].exports = oauthmock;

describe('abacus-provisioning-plugin plans creation', () => {
  let provisioning;

  beforeEach((done) => {
    delete require.cache[require.resolve('..')];
    provisioning = require('..');

    // Delete test dbs (plan and mappings) on the configured db server
    dbclient.drop(process.env.DB,
      /^abacus-rating-plan|^abacus-pricing-plan|^abacus-metering-plan/,
      done);
  });

  it('validates creation of new metering plans', (done) => {
    // Create a test provisioning app
    const app = provisioning();

    // Listen on an ephemeral port
    const server = app.listen(0);

    // Validate a valid test metering plan
    const mplan = {
      plan_id: 'test',
      measures: [
        {
          name: 'classifiers',
          unit: 'INSTANCE'
        }
      ],
      metrics: [
        {
          name: 'classifier_instances',
          unit: 'INSTANCE',
          type: 'discrete',
          formula: 'AVG({classifier})'
        }
      ]
    };

    let expected = 4;
    const checkDone = () => {
      expected--;
      if(expected === 0)
        done();
    };

    const validGetRequest = function(mplan) {
      request.get(
        'http://localhost::p/v1/metering/plans/:metering_plan_id', {
          p: server.address().port,
          metering_plan_id: mplan.plan_id
        }, (err, val) => {
          expect(err).to.equal(undefined);
          expect(val.statusCode).to.equal(200);
          expect(omit(val.body, 'id')).to.deep.equal(mplan);
          checkDone();
        });
    };

    const getFromCache = function(mplan) {
      request.get(
        'http://localhost::p/v1/metering/plans/:metering_plan_id', {
          p: server.address().port,
          metering_plan_id: mplan.plan_id
        }, (err, val) => {
          expect(err).to.equal(undefined);
          expect(val.statusCode).to.equal(200);
          expect(omit(val.body, 'id')).to.deep.equal(mplan);
          checkDone();
        });
    };

    const postRequest = function(mplan) {
      request.post(
        'http://localhost::p/v1/metering/plans/:metering_plan_id', {
          p: server.address().port,
          metering_plan_id: mplan.plan_id,
          body: mplan
        }, (err, val) => {
          expect(err).to.equal(undefined);
          expect(val.statusCode).to.equal(201);
          checkDone();
          validGetRequest(mplan);
          getFromCache(mplan);
        });
    };
    postRequest(mplan);
    request.post(
      'http://localhost::p/v1/metering/plans/:metering_plan_id', {
        p: server.address().port,
        metering_plan_id: 'test',
        body: {}
      }, (err, val) => {
        expect(err).to.equal(undefined);
        expect(val.statusCode).to.equal(400);
        checkDone();
      });
  });

  it('validates creation of new rating plans', (done) => {
    // Create a test provisioning app
    const app = provisioning();

    // Listen on an ephemeral port
    const server = app.listen(0);

    // Validate a valid test rating plan
    const rating = {
      plan_id: 'test',
      metrics: [
        {
          name: 'classifier',
          rate: ((price, qty) => new BigNumber(price || 0)
            .mul(qty).toNumber()).toString()
        }
      ]
    };

    let expected = 4;
    const checkDone = () => {
      expected--;
      if(expected === 0)
        done();
    };

    const getFromCache = function(rating) {
      request.get(
        'http://localhost::p/v1/rating/plans/:rating_plan_id', {
          p: server.address().port,
          rating_plan_id: rating.plan_id
        }, (err, val) => {
          expect(err).to.equal(undefined);
          expect(val.statusCode).to.equal(200);
          expect(omit(val.body, 'id')).to.deep.equal(rating);
          checkDone();
        });
    };

    const validGetRequest = function(rating) {
      request.get(
        'http://localhost::p/v1/rating/plans/:rating_plan_id', {
          p: server.address().port,
          rating_plan_id: rating.plan_id
        }, (err, val) => {
          expect(err).to.equal(undefined);
          expect(val.statusCode).to.equal(200);
          expect(omit(val.body, 'id')).to.deep.equal(rating);
          checkDone();
          getFromCache(rating);
        });
    };

    const postRequest = function(rating) {
      request.post(
        'http://localhost::p/v1/rating/plans/:rating_plan_id', {
          p: server.address().port,
          rating_plan_id: rating.plan_id,
          body: rating
        }, (err, val) => {
          expect(err).to.equal(undefined);
          expect(val.statusCode).to.equal(201);
          checkDone();
          validGetRequest(rating);
        });
    };
    postRequest(rating);
    request.post(
      'http://localhost::p/v1/rating/plans/:rating_plan_id', {
        p: server.address().port,
        rating_plan_id: rating.plan_id,
        body: {}
      }, (err, val) => {
        expect(err).to.equal(undefined);
        expect(val.statusCode).to.equal(400);
        checkDone();
      });
  });

  it('validates creation of new pricing plans', (done) => {
    // Create a test provisioning app
    const app = provisioning();

    // Listen on an ephemeral port
    const server = app.listen(0);

    // Validate a valid test pricing plan
    const pricing = {
      plan_id: 'test-db-basic',
      metrics: [
        {
          name: 'classifier',
          prices: [
            {
              country: 'USA',
              price: 0.00015
            },
            {
              country: 'EUR',
              price: 0.00011
            },
            {
              country: 'CAN',
              price: 0.00016
            }]
        }
      ]
    };

    let expected = 4;
    const checkDone = () => {
      expected--;
      if(expected === 0)
        done();
    };

    const getFromCache = function(pricing) {
      request.get(
        'http://localhost::p/v1/pricing/plans/:pricing_plan_id', {
          p: server.address().port,
          pricing_plan_id: pricing.plan_id
        }, (err, val) => {
          expect(err).to.equal(undefined);
          expect(val.statusCode).to.equal(200);
          expect(omit(val.body, 'id')).to.deep.equal(pricing);
          checkDone();
        });
    };

    const validGetRequest = function(pricing) {
      request.get(
        'http://localhost::p/v1/pricing/plans/:pricing_plan_id', {
          p: server.address().port,
          pricing_plan_id: pricing.plan_id
        }, (err, val) => {
          expect(err).to.equal(undefined);
          expect(val.statusCode).to.equal(200);
          expect(omit(val.body, 'id')).to.deep.equal(pricing);
          checkDone();
          getFromCache(pricing);
        });
    };

    const postRequest = function(pricing) {
      request.post(
        'http://localhost::p/v1/pricing/plans/:pricing_plan_id', {
          p: server.address().port,
          pricing_plan_id: pricing.plan_id,
          body: pricing
        }, (err, val) => {
          expect(err).to.equal(undefined);
          expect(val.statusCode).to.equal(201);
          checkDone();
          validGetRequest(pricing);
        });
    };
    postRequest(pricing);
    request.post(
      'http://localhost::p/v1/pricing/plans/:pricing_plan_id', {
        p: server.address().port,
        pricing_plan_id: pricing.plan_id,
        body: {}
      }, (err, val) => {
        expect(err).to.equal(undefined);
        expect(val.statusCode).to.equal(400);
        checkDone();
      });
  });
});
