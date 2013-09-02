var _ = require('underscore')
	, getNewDbDoc = require('./document').getNewDbDoc
	, dbUtil = require('./dbUtil');

 require('./_extensions');

var getSchemaDoc = function(coll){
  return _.find(coll,function(doc){
    return doc._meta.type === 'Schema';
  });
};
exports.getSchemaDoc = getSchemaDoc;

var getDataDocs = function(coll){
  return _.filter(coll,function(doc){
  	return doc._meta.type !== 'Schema' &&
    	doc._meta.type !== 'Transaction';
  });
};
exports.getDataDocs = getDataDocs;

var _schemaContainsType = function(db,tx,doc){
	return _.contains(db.Schema.Types,doc._meta.type);
};

var _docMustAssociateTransaction = function(db,tx,doc){
  var _hasLast = doc._meta.lastTransaction === tx._id;
  var _hasInList = _.contains(doc._meta.transactions,tx._id);
//  _.print(doc._meta.transactions);
//  if(!_hasLast) _.print('not has last');
//  if(!_hasInList) _.print('not has in list');
  return _hasLast && _hasInList;
};

var _docAlways = function(db,tx,doc){
  var _hasType = _schemaContainsType(db,tx,doc);
  var _hasTx = _docMustAssociateTransaction(db,tx,doc); 
//  if(!_hasType) _.print('not has type');
//  if(!_hasTx) _.print('not has tx');
  return _hasType && _hasTx;
};

//transaction must not be empty

var getNewSchemaDoc = function(dbName){

 	var toReturn = getNewDbDoc({});
 	_.extend(toReturn._meta,{
 		type: 'Schema',
 		isCommitted: false
 	});

 	toReturn.DbName = dbName;
 	toReturn.Types = ['Document','Transaction','Schema'];
 	toReturn.DbActions = ['upsert','delete'];
 	toReturn.Constraints = {
 		Document: {
 			atAllTimes: _docAlways.toString(), //invariant
 			onUpsert: _.always.toString(),
 			onDelete: _.always.toString()
 		},
 		Transaction:{
 			atAllTimes: _.always.toString(), //invariant
 			onUpsert: _.always.toString(),
 			onDelete: _.never.toString()
 		},
 		Schema:{
 			atAllTimes: _.always.toString(), //invariant
 			onUpsert: _.always.toString(),
 			onDelete: _.never.toString()
 		}
 	};

 	return toReturn;
 };
exports.getNewSchemaDoc = getNewSchemaDoc;


var getConstraintChecker = function(schemaDoc){
 	var toReturn = {};

 	_.each(schemaDoc.Constraints,function(_checks,_type){
 		var typeChecks = {};
 		_.each(_checks,function(_check,_dbEvent){
 			typeChecks[_dbEvent] = eval('(' + _check + ')');
 		});
 		toReturn[_type] = typeChecks;
 	});

 	toReturn.checkTxConstraints = function(db,tx){
    var _result = [];

    _.each(tx.actions,function(action){
      var _type = action.doc._meta.type;
      var _checks = toReturn[_type];      
      var _action = action.dbAction;

      if(!_checks){
        _result.push(_type + ' [' + action.doc._id + '] has no registered contraints in schema.  Must at least have all "always" constraints.');
        return;
      }
      if (!_checks.atAllTimes(db,tx,action.doc)){
        _result.push(_type + ' [' + action.doc._id + '] failed constraint: atAllTimes.');
      }
      if(_action !== 'delete' && _action !== 'upsert'){
        _result.push(_type + ' [' + action.doc._id + '] contains an unrecognized action of [' + _action + '].');
        return;
      }            
      if (_action === 'upsert' && !_checks.onUpsert(db,tx,action.doc)){
        _result.push(_type + ' [' + action.doc._id + '] failed constraint: onUpsert.');
      }
      if (_action === 'delete' && !_checks.onUpsert(db,tx,action.doc)){
        _result.push(_type + ' [' + action.doc._id + '] failed constraint: onDelete.');        
      }
    });

    return _result;
 	};
  /*
{
    "_meta": {
        "type": "Transaction",
        "dbStatus": "committedLocal"
    },
    "_id": "52243a9786e3718f3800000b",
    "actions": [
        {
            "dbAction": "upsert",
            "actionAdded": 1378106007619,
            "doc": {
                "test": "doc1",
                "_meta": {
                    "type": "Document",
                    "dbStatus": "committedBoth"
                },
                "_id": "52243a9786e3718f3800000c"
            }
        }
    ]
}
*/

 	return toReturn;
 };
 exports.getConstraintChecker = getConstraintChecker;