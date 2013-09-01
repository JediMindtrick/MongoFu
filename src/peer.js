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

exports.pushCommittedLocalTxToRemote = function(_conn,_db,callback){
	//get all committed locals
	_.print('getting all committed locals');
	var _txColl = _.every(_db.EventTransactions,dbUtil.isCommittedLocal);
	//push them to remote
	_.print('pushing to remote');
	transactor.pushTxToRemote(_conn,_db,_txColl,callback);
};