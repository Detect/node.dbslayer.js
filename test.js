/*
---
name: test.js
 
description: <
This is a demonstration of how dbslayer.js can be used.
It takes three parameters from the SQL query, a host
 
author: [Guillermo Rauch](http://devthought.com)
updated: [Andy Schuler](andy at leftshoedevelopment dot com)
updated: [Hao Chen](detectx at gmail dot com)
...
*/
require.paths.unshift(__dirname);

var sys = require('sys');
var dbslayer = require('dbslayer');
var args = process.ARGV.slice(2);
var db = new dbslayer.Server();
    
//basic query
var query = db.query.apply(db, args);
query.addListener("success", 
  function(result) {
    sys.puts('-------------------------');
    for (var i = 0, l = result.length; i < l; i++){
      sys.puts(JSON.stringify(result[i]));
    }
  }
);

query.addListener("error", 
  function(error, errno, object) {
    sys.puts('-------------------------');
    sys.puts('MySQL error (' + (errno || '') + '): ' + error);
    sys.puts('SQL: ' + object.SQL);
  }
);

['stat', 'client_info', 'host_info', 'server_version', 'client_version'].forEach(
  function(command){
    var el = db[command]();
    el.addListener("success",
      function(results) {
        sys.puts('-------------------------');    
        sys.puts(command.toUpperCase() + ' ' + results);
      }
    );
    
    el.addListener("error",
      function(results) {
        sys.puts('-------------------------');    
        sys.puts(command.toUpperCase() + ' ' + results);
      }
    );
  }
);
