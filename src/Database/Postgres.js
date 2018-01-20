
// module Database.Postgres

var pg = require('pg');
// https://github.com/brianc/node-postgres/issues/285
// Treating anything but base types as a string so we can do the deserializing in PS via fromSql
pg.types.setTypeParser(1082, function(v){ return String(v); }); // date
pg.types.setTypeParser(1083, function(v){ return String(v); }); // time
pg.types.setTypeParser(1114, function(v){ return String(v); }); // timestamp

exports.mkPool = function (conInfo) {
  return function () {
    return new pg.Pool(conInfo);
  };
}

exports["connect'"] = function (pool) {
  return function(error, success) {
    pool.connect(function(err, client) {
      if (err) {
        error(err);
      } else {
        success(client);
      }
    });
    return function(cancelError, onCancelerError, onCancelerSuccess) {
      onCancelerSuccess();
    };
  };
}

exports.runQuery_ = function(queryStr) {
  return function(client) {
    return function(error, success) {
      client.query(queryStr, function(err, result) {
        if (err) {
          error(err);
        } else {
          success(result.rows);
        }
      });
      return function(cancelError, onCancelerError, onCancelerSuccess) {
        onCancelerSuccess();
      };
    };
  };
}

exports.runQuery = function(queryStr) {
  return function(params) {
    return function(client) {
      return function(error, success) {
        client.query(queryStr, params, function(err, result) {
          if (err) return error(err);
          success(result);
        });
        return function(cancelError, onCancelerError, onCancelerSuccess) {
          onCancelerSuccess();
        };
      };
    };
  };
}

exports.runQueryValue_ = function(queryStr) {
  return function(client) {
    return function(error, success) {
      client.query(queryStr, function(err, result) {
        if (err) return error(err);
        success(result.rows.length > 0 ? result.rows[0][result.fields[0].name] : undefined);
      });
      return function(cancelError, onCancelerError, onCancelerSuccess) {
        onCancelerSuccess();
      };
    };
  };
}

exports.runQueryValue = function(queryStr) {
  return function(params) {
    return function(client) {
      return function(error, success) {
        client.query(queryStr, params, function(err, result) {
          if (err) return error(err);
          success(result.rows.length > 0 ? result.rows[0][result.fields[0].name] : undefined);
        });
        return function(cancelError, onCancelerError, onCancelerSuccess) {
          onCancelerSuccess();
        };
      };
    };
  };
}

exports.release = function (client) {
  return function () {
    client.release();
  };
}

exports.end = function(pool) {
  return function() {
    pool.end();
  };
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
  return function(row){
    return ["Problem row: " + JSON.stringify(row)]
      .concat(rawResult.fields.map(function(f){return JSON.stringify(f)}))
      .concat(["check https://github.com/brianc/node-pg-types/blob/master/lib/textParsers.js for info on parsers"])
      .join("\n");
  }
};
