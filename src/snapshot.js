var MongoClient = require('mongodb').MongoClient
 , _ = require('underscore')
 , ObjectID = require('mongodb').ObjectID
 , dbUtil = require('./dbUtil')
 , transaction = require('./transaction')
 , schema = require('./schema');

require('./_extensions');

//probably needs to return a deferred and implement accordingly
//var commitAllLocals = function(onError,onSuccess){
var pushTxToRemote = function(_conn,_db,_txColl,callback){
  var _dbName = _db.Schema.DbName;
  var added = 0;
  var errored = 0;
  var errorCollection = [];
  var insertedCollection = [];

  //create snapshot
  createNewSnapshot(
    _conn,
    _db,
    //success callback only, doesn't require error callback
    function(snapshot,schemaDoc){
      _.print('created snapshot, committing transaction');

      _loadDataIntoSnapshot(_db.EventTransactions,snapshot,function(){

          _.print('(transactor) committing snapshot');

          commitSnapshot(
            snapshot,
            schemaDoc,
            function(){

              _.print('snapshot committed, updating local');
              //update local copy
              _.each(_db.Data,dbUtil.markCommittedBoth);
              _.each(_db.EventTransactions,dbUtil.markCommittedBoth)
              _db.Schema._meta.isCommitted = true;
              dbUtil.markCommittedBoth(_db.Schema);
              _.print('finished with create new snapshot');

              callback(_db);
          });
      });
  });
};
exports.pushTxToRemote = pushTxToRemote;

var commitSnapshot = function(snapshot,schemaDoc,callback){

  schemaDoc._meta.isCommitted = true;
  _.print('inside commitSnapshot()');
  _.print(schemaDoc);
  _.print('snapshot has save?: ' + (snapshot.save ? true : false));
  snapshot.save(
    dbUtil.markCommittedBoth(schemaDoc),
    function(err,result){
      _.print('schema saved callback');
      if(err){
        _.print('(transactor) error committing snapshot, aborting');
        _.print('(transactor) error: ' + err);

        callback(err);
        return;
      }

      _.print('schema saved with isCommitted = true');

      callback();
  });
};

var destroySnapshot = function(snapshot,onSuccess){
  db.dropCollection(snapshot.options.create,
    function(err){
    onSuccess();
  });
};

var getUniqueId = function(){
  return dbUtil.getUniqueId();
};

var copyDoc = function(toCopy){
  return dbUtil.copyDoc(toCopy);
};

var _loadDataIntoSnapshot = function(dataSet,snapshot,callback){
  var toSave = _.size(dataSet);
  var _saved = 0;

  if(toSave < 1){
    _.print('nothing to load into snapshot');
    callback();
  }

  _.each(dataSet,function(doc){
    _.print('saving doc:');
    _.print(doc);
    snapshot.save(
      copyDoc(dbUtil.markCommittedBoth(doc)),
      function(err,doc){
        _saved++;

        if(err){
          _.print('error:');
          _.print(err);
          throw err;
          return;
        }

        if(_saved === toSave){
          callback();          
        }
      });
  });
};

var createNewSnapshot = function(_conn,_db,callback){

  var snapName = _db.Schema.DbName + '-' + getUniqueId();
  _.print('Creating "' + snapName + '"');

  _conn.createCollection(
    snapName,
    function(err, snapshot){
      if (err) throw err;

      var schemaDoc = copyDoc(_db.Schema);
      schemaDoc._meta.isCommitted = false;

      snapshot.save(
        schemaDoc,
        function(err,doc){
          if(err) throw err;

          _loadDataIntoSnapshot(_db.Data,snapshot,function(){
            callback(snapshot, schemaDoc);  
          });          

        });//save schema doc
  });//create collection
};