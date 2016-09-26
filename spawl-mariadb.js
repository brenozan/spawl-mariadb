// Persistence Abstraction module
// SPAWL - Simple Persistence Abstraction Awsome Layer - MySQL/MariaDB
// Created by Eduardo Quagliato <eduardo@quagliato.me>
// Date: 2015-09-23
// Location: Curitiba, Brasil

var MariaDB                  = require("./mariadb");

function SpawlMariaDBConnector(dbConfig, logger){
  this.mariadb = new MariaDB(dbConfig, logger);
};

var encapsulatedTypes = ['string'];

SpawlMariaDBConnector.prototype.get = function(entity, fields, filter, order, page, size, callback){
  var sql = "SELECT";
  var fieldsStr = "";

  if (fields === undefined || fields.length === 0) {
    fieldsStr = "*";
  } else {
    for (var i = 0; i < fields.length; i++) {
      if (fieldsStr.length > 0) fieldsStr = "{0}, ".format(fieldsStr);
      fieldsStr = "{0}{1}".format(fieldsStr, fields[i]);
    }
  }

  sql = "{0} {1} FROM {2}".format(sql, fieldsStr, entity);

  var whereStr = "";
  console.log(entity + ": " + JSON.stringify(filter));
  if (filter !== undefined && filter.hasOwnProperty("filter")) {
    whereStr = createWhere(whereStr, filter["filter"]);
    sql = "{0} WHERE {1}".format(sql, whereStr);
  }

  var orderStr = "";

  if (order !== undefined && order.hasOwnProperty("fields") && order["fields"].length > 0) {
    var destiny = "down";
    if (order.hasOwnProperty("destiny") && (order["destiny"] == "up" || order["destiny"] == "down")) {
      destiny = order["destiny"];
    }

    for (var i = 0; i < order["fields"].length; i++) {
      if (orderStr.length > 0) orderStr = "{0}, ".format(orderStr);
      orderStr = "{0} {1} {2}".format(orderStr, order["fields"][i], (destiny == "up" ? "DESC" : "ASC"));
    }

    sql = "{0} ORDER BY {1}".format(sql, orderStr);
  }

  if ((size !== undefined && size > 0) && (page !== undefined && page > 0)) {
    sql = "{0} LIMIT {1} OFFSET {2}".format(sql, (size * page), (size * (page - 1)));
  }

  this.mariadb.query(sql, function(size, rows){
    callback(size, rows);
  });
};

SpawlMariaDBConnector.prototype.update = function(entity, object, fields, filter, callback){
  var sql = "UPDATE {0} SET".format(entity);
  var fieldsStr = "";

  if (fields !== undefined && fields.length > 0) {
    for (var i = 0; i < fields.length; i++) {
      if (fieldsStr.length > 0) fieldsStr = "{0}, ".format(fieldsStr);
      if (encapsulatedTypes.indexOf(typeof object[fields[i]]) >= 0) {
        fieldsStr = "{0}{1} = '{2}'".format(fieldsStr, fields[i], object[fields[i]]);
      } else {
        fieldsStr = "{0}{1} = {2}".format(fieldsStr, fields[i], object[fields[i]]);
      }
    }
  } else {
    for (var key in object) {
      if (fieldsStr.length > 0) fieldsStr = "{0}, ".format(fieldsStr);
      if (encapsulatedTypes.indexOf(typeof object[key]) >= 0) {
        fieldsStr = "{0}{1} = '{2}'".format(fieldsStr, key, object[key]);
      } else {
        fieldsStr = "{0}{1} = {2}".format(fieldsStr, key, object[key]);
      }
    }
  }

  sql = "{0} {1}".format(sql, fieldsStr);

  var whereStr = "";

  if (filter !== undefined && filter.hasOwnProperty("filter")) {
    whereStr = createWhere(whereStr, filter["filter"]);
    sql = "{0} WHERE {1}".format(sql, whereStr);
  }

  this.mariadb.query(sql, function(size, rows){
    callback(size, rows);
  });
};

SpawlMariaDBConnector.prototype.save = function(entity, object, callback){
  var sql = "INSERT INTO {0}".format(entity);
  var fieldsStr = "";
  var valuesStr = "";

  var modelObject = object;

  if (object.hasOwnProperty("length")) modelObject = object[0];

  for (var key in modelObject) {
    if (fieldsStr.length > 0) fieldsStr = "{0}, ".format(fieldsStr);
    fieldsStr = "{0}{1}".format(fieldsStr, key);

    if (!object.hasOwnProperty("length")) {
      if (valuesStr.length > 0) valuesStr = "{0}, ".format(valuesStr);
      if (encapsulatedTypes.indexOf(typeof object[key]) >= 0) {
        valuesStr = "{0}'{1}'".format(valuesStr, object[key]);
      } else {
        valuesStr = "{0}{1}".format(valuesStr, object[key]);
      }
    }
  }

  if (object.hasOwnProperty("length")) {
    for (var i = 0; i < object.length; i++) {
      if (valuesStr.length > 0) valuesStr = "{0},".format(valuesStr);
      var actualObject = object[i];

      var oneEntryStr = "";
      for (var key in actualObject) {
        if (oneEntryStr.length > 0) oneEntryStr = "{0}, ".format(oneEntryStr);
        if (encapsulatedTypes.indexOf(typeof actualObject[key]) >= 0) {
          oneEntryStr = "{0}'{1}'".format(oneEntryStr, actualObject[key]);
        } else {
          oneEntryStr = "{0}{1}".format(oneEntryStr, actualObject[key]);
        }
      }

      valuesStr = "{0}({1})".format(valuesStr, oneEntryStr);
    }

    sql = "{0}({1}) VALUES{2}".format(sql, fieldsStr, valuesStr);
  } else {
    sql = "{0}({1}) VALUES({2})".format(sql, fieldsStr, valuesStr);
  }

  this.mariadb.query(sql, function(size, rows){
    callback(size, rows);
  });
};

SpawlMariaDBConnector.prototype.delete = function(entity, filter, callback){
  var sql = "DELETE FROM {0}".format(entity);

  var whereStr = "";

  if (filter !== undefined && filter.hasOwnProperty("filter")) {
    whereStr = createWhere(whereStr, filter["filter"]);
    sql = "{0} WHERE {1}".format(sql, whereStr);
  }

  this.mariadb.query(sql, function(size, rows){
    callback(size, rows);
  });
};

var createWhere = function (str, filter) {
  if (filter.hasOwnProperty("unifier")) {
    var unifier = filter.unifier;
    var values = filter.values;
    var piece = "";
    for (var key in values) {
      var valueEntry = values[key];
      if (piece.length > 0) piece = "{0} {1} ".format(piece, unifier);

      if (valueEntry.hasOwnProperty("field") && valueEntry.hasOwnProperty("operator") && valueEntry.hasOwnProperty("value")) {
        if (encapsulatedTypes.indexOf(typeof valueEntry.value) >= 0 && valueEntry.operator != "IN") {
          piece = "{0} {1} {2} '{3}'".format(piece, valueEntry.field, valueEntry.operator, valueEntry.value);
        } else {
          piece = "{0} {1} {2} {3}".format(piece, valueEntry.field, valueEntry.operator, valueEntry.value);
        }
      } else if (valueEntry.hasOwnProperty("unifier")) {
        piece = "{0} ({1})".format(piece, createWhere(piece, valueEntry));
      }
    }

    return piece;
  } else if (filter.hasOwnProperty("field")) {
    if (encapsulatedTypes.indexOf(typeof filter.value) >= 0 && filter.operator != "IN") {
      return "{0} {1} '{2}'".format(filter.field, filter.operator, filter.value);
    } else {
      return "{0} {1} {2}".format(filter.field, filter.operator, filter.value);
    }

  }
};

module.exports = SpawlMariaDBConnector;

