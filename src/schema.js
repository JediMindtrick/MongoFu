var _ = require('underscore')
 , dbUtil = require('./dbUtil');
 require('./_extensions');

 var getSchemaDoc = function(coll){
  return _.find(coll,function(doc){
    return doc._meta.type === 'Schema';
  });
};
exports.getSchemaDoc = getSchemaDoc;

var getNewSchemaDoc = function(){

 	var toReturn = dbUtil.getNewDbDoc({});
 	_.extend(toReturn._meta,{
 		type: 'Schema',
 		isCommitted: true
 	});

 	toReturn.Types = ['Document','Transaction','Schema'];
 	toReturn.Constraints = {
 		Document: {
 			atAllTimes: (_.always).toString(), //invariant
 			onUpsert: (function(coll,tx,doc){
 				return _.contains(getSchemaDoc(coll).Types,doc._meta.type) ||
 					_.contains(getSchemaDoc(tx).Types,doc._meta.type);
 			}).toString(),
 			onDelete: (_.always).toString()
 		},
 		Transaction:{
 			atAllTimes: (_.always).toString(), //invariant
 			onUpsert: (_.always).toString(),
 			onDelete: (_.always).toString()
 		},
 		Schema:{
 			atAllTimes: (_.always).toString(), //invariant
 			onUpsert: (function(coll,tx,doc){
 				return _.contains(getSchemaDoc(coll).Types,doc._meta.type);
 			}).toString(),
 			onDelete: (_.always).toString()
 		}
 	};

 	return toReturn;
 };
 exports.getNewSchemaDoc = getNewSchemaDoc;