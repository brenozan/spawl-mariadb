// MariaDB module
// Created by Eduardo Quagliato <eduardo@quagliato.me>
// Date: 2015-07-29
// Location: Curitiba, Brasil

var moment                   = require('moment');
var mysql                    = require('mysql');

var _filled = function(value){
  if (value !== null && value !== undefined) {
    if (typeof value === "string" && value !== "") return true;
    if (typeof value === "function") return true;
    if (typeof value === "boolean" && (value === true || value === false)) return true;
    if (typeof value === "number") return true;
    if (typeof value === "object") {
      if (value.hasOwnProperty("length") && value.length > 0) return true;
      var count = 0;
      for (var prop in value) {
        count++;
      }
      if (count > 0) return true;
    }
  }
  return false;
}

var MariaDB = function(sessionConfig, sessionLogger){
  if (!_filled(sessionConfig.DB_HOST) ||
    !_filled(sessionConfig.DB_USER) ||
    !_filled(sessionConfig.DB_PASS) ||
    !_filled(sessionConfig.DB_NAME)) {

    return undefined;
  }

  this.dbConfig = {
    "DB_HOST": sessionConfig.DB_HOST,
    "DB_USER": sessionConfig.DB_USER,
    "DB_PASS": sessionConfig.DB_PASS,
    "DB_NAME": sessionConfig.DB_NAME
  };

  if (_filled(sessionLogger) && typeof sessionLogger === "function") {
    this.logger = sessionLogger
  }

  mariadbObj = this;

  this.connectDB = function(callback){
    if (mariadbObj.dbConnection === null || mariadbObj.dbConnection === undefined) {
      mariadbObj.dbConnection = mysql.createConnection({
        host     : mariadbObj.dbConfig.DB_HOST,
        user     : mariadbObj.dbConfig.DB_USER,
        password : mariadbObj.dbConfig.DB_PASS,
        database : mariadbObj.dbConfig.DB_NAME
      });
      mariadbObj.logger("Instanced MariaDB Connection.");

      mariadbObj.dbConnection.connect();
      mariadbObj.logger("Connected to MariaDB Server");

      callback(mariadbObj.dbConnection);
      return;
    }

    callback(mariadbObj.dbConnection);
  };

  this.disconnectDB = function(){
    mariadbObj.logger("Connection to MariaDB Server ended.");
    mariadbObj.dbConnection.end();
  };

  this.query = function(queryStr, callback){
    mariadbObj.logger("SQL: {0}".format(queryStr));
    mariadbObj.connectDB(function(connection){
      connection.query(queryStr, function(err, rows, fields) {
        if (err) {
          mariadbObj.logger("SQL: {0} - {1}".format(queryStr, err), "CRITICAL");
          callback(-1);
        } else {
          var size = 0;
          var is_select = true;
          if (rows.affectedRows>0) { // Insert, update, delete
            is_select = false;
          } else if (rows && rows != null && rows.length > 0) {
            size = rows.length;
          }

          var result = [];

          if (size > 0) {
            for (var i = 0; i < rows.length; i++) {
              var newRow = {};
              for (var j = 0; j < fields.length; j++) {
                newRow[fields[j].name]= rows[i][fields[j].name]
              }
              result[result.length] = newRow
            }
          }

          if (size === 1) result = result[0];

          callback(parseInt(size), result);
        }
      });
    });
  };
};

module.exports = MariaDB;

