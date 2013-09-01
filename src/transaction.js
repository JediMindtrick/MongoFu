var _ = require('underscore')
 , dbUtil = require('./dbUtil');

require('./_extensions');

var noOp = dbUtil.noOp;

var getTransactionDocs = function(coll){
  return _.where(coll,function(doc){
    return doc._meta.type === 'Transaction';
  });
};
exports.getTransactionDocs = getTransactionDocs;

var createTransaction = function(/*actions*/){
	var actions = _.toArray(arguments);

	var toReturn = dbUtil.getNewDbDoc({});
	toReturn._meta.type = 'Transaction';
	toReturn.actions = [];
	_.each(actions,function(action){

		var _newDoc = action;

		if(!action._meta){
			_newDoc = dbUtil.getNewDbDoc(_newDoc);
		}

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

var runActions = function(db,actions,onError,onSuccess){
	if(actions.length === 0) {
		onSuccess();
		return;
	}
	//process and recur	
	dbUtil.upsertToRemote(
		db,
		actions[0].doc,
		onError,
		function(){
			runActions(db,_.rest(actions),onError,onSuccess);
		});
};

var runTransaction = function(db,txDb,tx,onError,onSuccess){

	console.log(JSON.stringify(tx.actions));

	//sort earliest first
	var _sorted = _.sortBy(
		tx.actions,
		function(action){
			return action.actionAdded;
	});

	console.log('sorted: ');
	console.log(JSON.stringify(_sorted));

	//need to do this in order, and take into account async nature of upsert
	runActions(
		db,
		_sorted,
		onError,
		function(){
			dbUtil.upsertToRemote(txDb,tx,onError,onSuccess);
		});	
};
exports.runTransaction = runTransaction;

var runTransactionAgainstLocal = function(tx,dataSet){	
	//find every item, and mark it as committed
	_.each(
		tx.actions,
		function(action){
			var _doc = action.doc;
			//find and replace it in the dataSet
			var _found = _.find(dataSet,function(item){
				return item._id === _doc._id;
			});

			if(_found){
				dbUtil.markCommitted(_found);
			}else{
				dbUtil.markCommitted(_doc);
				dataSet.push(_doc);
			}
	});

	//insert the tx itself
	dataSet.push(dbUtil.markCommitted(tx));
};
exports.runTransactionAgainstLocal = runTransactionAgainstLocal;