var _ = require('underscore')
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

var _schemaContainsType = function(_db,tx,doc){
	return _.contains(_db.Schema.Types,doc._meta.type);
};

var getNewSchemaDoc = function(dbName){

 	var toReturn = dbUtil.getNewDbDoc({});
 	_.extend(toReturn._meta,{
 		type: 'Schema',
 		isCommitted: false
 	});

 	toReturn.DbName = dbName;
 	toReturn.Types = ['Document','Transaction','Schema'];
 	toReturn.Constraints = {
 		Document: {
 			atAllTimes: _schemaContainsType.toString(), //invariant
 			onUpsert: _schemaContainsType.toString(),
 			onDelete: _.always.toString()
 		},
 		Transaction:{
 			atAllTimes: _.always.toString(), //invariant
 			onUpsert: _.always.toString(),
 			onDelete: _.always.toString()
 		},
 		Schema:{
 			atAllTimes: _.always.toString(), //invariant
 			onUpsert: _.always.toString(),
 			onDelete: _.always.toString()
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

 	return toReturn;
 };
 exports.getConstraintChecker = getConstraintChecker;