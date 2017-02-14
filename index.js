var _ = require('lodash');
var elasticsearch = require('elasticsearch');
var moment = require('moment');

var apiVersion = process.env.API_VERSION;
var awsRegion = process.env.AWS_REGION;
var batchSize = parseInt(process.env.BATCH_SIZE) || 10;
var endpoint = process.env.ENDPOINT;
var excludedIndices = (process.env.EXCLUDED_INDICES || '.kibana').split(/[ ,]/);
var indexDate = moment.utc().subtract(+(process.env.MAX_INDEX_AGE || 14), 'days');
var logLevel = process.env.LOG_LEVEL || 'info';
var timeout = process.env.ES_TIMEOUT;

if (awsRegion !== undefined) {
  var AWS = require('aws-sdk');
}

exports.handler = function(event, context, callback) {
  var config = {
    apiVersion: apiVersion,
    host: endpoint,
    log: logLevel,
    requestTimeout: timeout,
  };

  if (awsRegion !== undefined) {
    config = _.merge(config, {
      amazonES: {
        credentials: new AWS.EnvironmentCredentials('AWS'),
        region: awsRegion,
      },
      connectionClass: require('http-aws-es'),
    });
  }

  var client = new elasticsearch.Client(config);

  getIndices(client)
    .then(extractIndices)
    .then(filterIndices)
    .then(deleteIndices(client))
    .then(report(callback), callback);
}

function getIndices(client) {
  return client.cat.indices({h: ['index']});
}

function extractIndices(results) {
  return results.split('\n')
}

function filterIndices(indices) {
  return indices.filter(function(index) {
    return !isExcluded(index) && (isTooOld(index) || isTooNew(index));
  });
}

function deleteIndices(client) {
  return function(indices) {
    if (indices.length > 0) {
      promises = _.map(_.chunk(indices, batchSize), function (chunk) {
        return client.indices.delete({ index: chunk });
      });

      return Promise.all(promises).then(function(succeeded) {
        return indices;
      });
    } else {
      return indices;
    }
  };
}

function report(callback) {
  return function(indices) {
    var len = indices.length;
    if (len > 0) {
      callback(null, 'Successfully deleted ' + len + ' indices: ' + indices.join(', '));
    } else {
      callback(null, 'There were no indices to delete.');
    }
  };
}

function isExcluded(indexName) {
  return excludedIndices.indexOf(indexName) !== -1;
}

function isTooOld(indexName) {
  var m = moment.utc(indexName, 'YYYY.MM.DD');
  return m.isBefore(indexDate);
}

function isTooNew(indexName) {
  var maxDate = moment.utc().add(2, 'days');
  var m = moment.utc(indexName, 'YYYY.MM.DD');
  return m.isAfter(maxDate);
}
