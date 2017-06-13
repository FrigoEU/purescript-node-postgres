
// module Database.Postgres

var pg = require('pg');
// https://github.com/brianc/node-postgres/issues/285
// Treating anything but base types as a string so we can do the deserializing in PS via fromSql
pg.types.setTypeParser(1082, function(v){ return String(v); }); // date
pg.types.setTypeParser(1083, function(v){ return String(v); }); // time
pg.types.setTypeParser(1114, function(v){ return String(v); }); // timestamp

exports["connect'"] = function (conString) {
  return function(success, error) {
    var client = new pg.Client(conString);
    client.connect(function(err) {
      if (err) {
        error(err);
      } else {
        success(client);
      }
    })
    return client;
  };
}

exports._withClient = function (conString, cb) {
  return function(success, error) {
    pg.connect(conString, function(err, client, done) {
      if (err) {
        done(true);
        return error(err);
      }
      cb(client)(function(v) {
        done();
        success(v);
      }, function(err) {
        done();
        error(err);
      })
    });
  };
}

exports.runQuery_ = function (queryStr) {
  return function(client) {
    return function(success, error) {
      client.query(queryStr, function(err, result) {
        if (err) return error(err);
        success(result);
      })
    };
  };
}

exports.runQuery = function (queryStr) {
  return function(params) {
    return function(client) {
      return function(success, error) {
        client.query(queryStr, params, function(err, result) {
          if (err) return error(err);
          success(result);
        })
      };
    };
  };
}

exports.runQueryValue_ = function (queryStr) {
  return function(client) {
    return function(success, error) {
      client.query(queryStr, function(err, result) {
        if (err) return error(err);
        success(result.rows.length > 0 ? result.rows[0][result.fields[0].name] : undefined);
      })
    };
  };
}

exports.runQueryValue = function (queryStr) {
  return function(params) {
    return function(client) {
      return function(success, error) {
        client.query(queryStr, params, function(err, result) {
          if (err) return error(err);
          success(result.rows.length > 0 ? result.rows[0][result.fields[0].name] : undefined);
        })
      };
    };
  };
}

exports.end = function (client) {
  return function() {
    client.end();
  };
}

exports.disconnect = function () {
  pg.end();
}

exports.isObjectWithAllNulls = function(fn){
  var keys, i, acc;
  if (typeof fn === "object"){
    keys = Object.keys(fn);
    acc = true;
    for (i = 0; acc && i < keys.length; i++){
      if (fn[keys[i]] !== null){ acc = false; }
    }
    return acc;
  } else {
    return false;
  }
};

exports.showDiagnostics = function(rawResult){
  return JSON.stringify(rawResult) + "\ncheck https://github.com/brianc/node-pg-types/blob/master/lib/textParsers.js for info on parsers";
};
