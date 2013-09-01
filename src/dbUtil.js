var _ = require('underscore')
 , ObjectID = require('mongodb').ObjectID;
require('./_extensions');

exports.noOp = _.noOp;

var markCommittedLocal = function(doc){
  doc._meta.dbStatus = 'committedLocal';
  return doc;
};
exports.markCommittedLocal = markCommittedLocal;

var isCommittedLocal = function(doc){
  return doc._meta.dbStatus === 'committedLocal';
};
exports.isCommittedLocal = isCommittedLocal;

var markCommittedBoth = function(doc){
  doc._meta.dbStatus = 'committedBoth';
  return doc;
};
exports.markCommittedBoth = markCommittedBoth;

var isCommittedBoth = function(doc){
  return doc._meta.dbStatus === 'committedBoth';
};
exports.isCommittedBoth = isCommittedBoth;

var copyDoc = function(toCopy){
  return JSON.parse(JSON.stringify(toCopy));
};
exports.copyDoc = copyDoc;

var getUniqueId = function(){
  return (new ObjectID()).toHexString();
};
exports.getUniqueId = getUniqueId;

var getNewDbDoc = function(toInsert){
  toInsert._meta = {
    type: 'Document',
    dbStatus: 'uncommitted' 
  };

  toInsert._id = getUniqueId();

  return toInsert;
};
exports.getNewDbDoc = getNewDbDoc;

var upsertToRemote = function(coll,toInsert,onError,onSuccess){
  var newInsert = copyDoc(toInsert);

  newInsert = markCommitted(newInsert);

  coll.save( 
    newInsert,
    function(err,doc){
      if(err) onError(err);

      toInsert = markCommitted(toInsert);

      onSuccess(toInsert);
  });
};
exports.upsertToRemote = upsertToRemote;

var getDocTimestamp = function(doc){
	return ObjectID(doc._id).getTimestamp().getTime();
};
exports.getDocTimestamp = getDocTimestamp;