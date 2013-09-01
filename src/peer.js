var MongoClient = require('mongodb').MongoClient
 , _ = require('underscore')
 , ObjectID = require('mongodb').ObjectID
 , dbUtil = require('./dbUtil')
 , transaction = require('./transaction')
 , schema = require('./schema')
 , transactor = require('./transactor');

require('./_extensions');

exports.loadHead = transactor.loadHead;
exports.runTxLocal = transaction.runTransactionAgainstLocal;

exports.pushCommittedLocalTxToRemote = function(conn,db,callback){
	//get all committed locals
	var _tx = _.every(db.EventTransactions,dbUtil.isCommittedLocal);
	//push them to remote
	transactor.pushTxToRemote(conn,db.Schema.DbName,callback);
};