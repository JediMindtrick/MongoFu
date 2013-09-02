var _ = require('underscore')
 , dbUtil = require('./dbUtil')
 , getNewDbDoc = require('./document').getNewDbDoc;

require('./_extensions');

var noOp = dbUtil.noOp;

var getTransactionDocs = function(coll){
  return _.filter(coll,function(doc){
    return doc._meta.type === 'Transaction';
  });
};
exports.getTransactionDocs = getTransactionDocs;

var createTransaction = function(/*actions*/){
	var actions = _.toArray(arguments);

	var toReturn = getNewDbDoc({});
	var _txId = toReturn._id;
	toReturn._meta.type = 'Transaction';
	toReturn.actions = [];
	_.each(actions,function(action){

		var _newDoc = action;

		if(!action._meta){
			_newDoc = getNewDbDoc(_newDoc);
		}

//		_newDoc._meta.transactions = _newDoc._meta.transactions || [];
		_newDoc._meta.transactions.push(_txId);
		_newDoc._meta.lastTransaction = _txId;

		var newAction = {
			dbAction: action.dbAction ? action.dbAction : 'upsert',
			actionAdded: (new Date()).getTime(),
			doc: _newDoc
		};
		toReturn.actions.push(newAction);
	});

	return toReturn;
};
exports.createTransaction = createTransaction;

var runTransactionAgainstLocal = function(db,tx){

	//find every item, and mark it as committed
	_.each(
		tx.actions,
		function(action){
			var _doc = action.doc;
			//find and replace it in the dataSet
			var _found = _.find(db.Data,function(item){
				return item._id === _doc._id;
			});

			_doc = _found ? _found : _doc;

			dbUtil.markCommittedLocal(_doc);
			db.Data.push(_doc);
		});

	//insert the tx itself
	db.EventTransactions.push(dbUtil.markCommittedLocal(tx));
};
exports.runTransactionAgainstLocal = runTransactionAgainstLocal;