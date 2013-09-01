var MongoClient = require('mongodb').MongoClient
 , _ = require('underscore')
 , ObjectID = require('mongodb').ObjectID
 , peer = require('./transactor')
 , transact = require('./transaction');

var fullName = function(f,l){
	return f + ' ' + l;
};

//this works
var strFunc = '(function(first,last){' +
"	var unique = require('./dbUtil').getUniqueId;" +
"	console.log(unique());" +
'})';

//this works too
var strFunc2 = '(function(first,last){' +
'	console.log(fullName(first,last));' +
'})';

var printFull = eval(strFunc2);

printFull('Brandon', 'Wilhite');