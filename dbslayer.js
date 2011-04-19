/*
---
name: dbslayer.js
 
description: Interface to DBSlayer for Node.JS
 
author: [Guillermo Rauch](http://devthought.com)
updated: Andy Schuler (andy at leftshoedevevelopment dot com)
updated: Hao Chen (detect at hotmail dot com)
...
*/

var sys = require('sys');
var http = require('http');
var events = require('events');

var booleanCommands = ['STAT', 'CLIENT_INFO', 'HOST_INFO', 'SERVER_VERSION', 'CLIENT_VERSION'];

var Server = this.Server = function(host, port, timeout){
  this.host = host || 'localhost';
  this.port = port || 9090;
  this.timeout = timeout;
};

Server.prototype.fetch = function(obj, key){
  var e = new events.EventEmitter();
  var connection = http.createClient(this.port, this.host);

  connection.addListener('error', function(error) {
   e.emit('error', 'DBSlayer error: ' + error.message, error.errno, obj);
  });

  var request = connection.request("GET",'/db?' + escape(JSON.stringify(obj)), {'host': this.host});

  request.addListener('response',
    function(response) {
      var data = [];
      response.addListener('data',
        function(chunk) {
          data.push(chunk);
        }
      );
      response.addListener('end',
        function() {
          try {
            var object = JSON.parse(data.join(''));
            if(key == 'OBJECT' && object.MYSQL_ERROR === undefined)
            {
              object['OBJECT'] = [];
              var results = object['RESULT'];
              var column_names = results['HEADER'];
              var rows = results['ROWS'];
              for(row in rows) {
                var newrow = {};
                for(column in column_names) {
                   var value = rows[row][column];
                   try {
                     value = JSON.parse(value);
                   } catch (e) {};
                   newrow[column_names[column]] = value;
                }
                object['OBJECT'].push(newrow);
              }
            }
          }
          catch(err) {
            e.emit('error', err, "Unknown", obj);
            return;
          }
          if (object.MYSQL_ERROR !== undefined){
            e.emit('error', object.MYSQL_ERROR, object.MYSQL_ERRNO, obj);
            return;
          } 
          if (object.ERROR !== undefined){
            e.emit('error', object.ERROR, "Unknown", obj);
            return;
          } 
          e.emit('success', key ? object[key] : object, obj);
        }
      );
    }
  );
  request.end();
  return e;
}

Server.prototype.query = function(query){
  if(arguments.length > 1) {
    if(arguments instanceof Array) {
      var args = arguments;
    } else {
      var args = Array.prototype.slice.call(arguments);
    }
    query = this.formatQuery(query, args.slice(1));
  }
  return this.fetch({SQL: query}, 'OBJECT');
};

Server.prototype.formatQuery = function(query, args) {
  var escapeQuery = this.escapeQuery;
  query = query.replace(/\?/g, function() {
    return escapeQuery(args.shift());
  });
  return query;
};

Server.prototype.escapeQuery = function(val) {
  if (val === undefined || val === null) {
    return 'NULL';
  }

  switch (typeof val) {
    case 'boolean': return (val) ? 'true' : 'false';
    case 'number': return val+'';
  }

  if (typeof val === 'object') {
    try {
      val = JSON.stringify(val);
    } catch(e) {
      val = val.toString();
    }
  }

  val = val.replace(/[\0\n\r\b\t\\\'\"\x1a]/g, function(s) {
    switch(s) {
      case "\0": return "\\0";
      case "\n": return "\\n";
      case "\r": return "\\r";
      case "\b": return "\\b";
      case "\t": return "\\t";
      case "\x1a": return "\\Z";
      default: return "\\"+s;
    }
  });
  return "'"+val+"'";
};

for (var i = 0, l = booleanCommands.length; i < l; i++){
  Server.prototype[booleanCommands[i].toLowerCase()] = (
    function(command){
      return function(){
        var obj = {};
        obj[command] = true;
        return this.fetch(obj, command);
      };
    }
  )(booleanCommands[i]);
}
