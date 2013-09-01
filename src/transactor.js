var MongoClient = require('mongodb').MongoClient
 , _ = require('underscore')
 , ObjectID = require('mongodb').ObjectID
 , dbUtil = require('./dbUtil')
 , transaction = require('./transaction')
 , schema = require('./schema');

require('./_extensions');

// URL for MongoDB
var url = '';
var db = null;
var RemoteDb = null;
var LocalDb = {
  Data: [],
  Transactions: [],
  Schema: {}
};
var dbName = '';

var setUrl = function(_url){
  url = _url;
};

var setCollection = function(collName){
  dbName = collName;
};

var connect = function(_url,onSuccess){
  _.print('Connecting to MongoDB...');
  setUrl(_url);

  MongoClient.connect(url, function (err, database) {
    _.print('Attempting to connect to ' + url);

    if (err) throw err;
    db = database;

    _.print('Connected to MongoDB');

    onSuccess(database);
  });
};
exports.connect = connect;

var getRemote = function(){
  return RemoteDb;
};
exports.getRemote = getRemote;

var initializeBase = function(baseName,onSuccess,_db){
    var _baseName = baseName + '-Base-' + getUniqueId();
    _.print('Initializing base for ' + _baseName);

    return initializeCollection(
        _db,
        _baseName,
        baseName,
        function(){
          _.print('Initialized base for ' + _baseName);
          _.safeInvoke(onSuccess);
        });
};
exports.initializeBase = initializeBase;

var initializeCollection = function(_conn,collName,dbName,onSuccess){

  _conn.createCollection(collName, function(err, _db){
    if (err){
      _.print('problem creating collection ' + collName);
      throw err;
    }
    _.print('collection created ' + collName);
    RemoteDb = _db;

    var _schemaDoc = schema.getNewSchemaDoc(dbName);

    _db.save(
      _schemaDoc,
      function(err,doc){
        if(err) throw err;
        _.print('successfully initialized ' + dbName);
        onSuccess(_schemaDoc);
    });
  });
};
exports.initializeCollection = initializeCollection;

var deleteCollection = function(collName,onSuccess,_conn){
    var _toRunDb = _conn || db;

    _toRunDb.dropCollection(collName, function(err){
    if(err) throw err;
    onSuccess();
  });
};
exports.deleteCollection = deleteCollection;

var connectToCollection = function(_url,collName,onSuccess){
  setCollection(collName);

  _.print('Loading data for ' + dbName);

  connect(
    _url,
    function(){
      loadHead(function(){
        _.print('Data loaded.');
        onSuccess(LocalDb.Data);
      });
  });
};
exports.connectToCollection = connectToCollection;

var getSnapshot = function(version,onSuccess){

  var collection = db.collection(dbName + '-' + version);
  snapshot = collection;

  snapshot.find({}).toArray(function(err,docs){
    onSuccess(docs);
  });
};
exports.getSnapshot = getSnapshot;

var close = function(aDb){

  _.print('closing...');
  if(aDb){
      aDb.close();
  }else{
      db.close();
  }
  _.print('closed');
};
exports.close = close;

var listAllCollections = function(onSuccess,_conn){
  var _toRunDb = _conn || db;

  _toRunDb.collectionNames(function(err, collections){
    onSuccess(collections);
  });
};
exports.listAllCollections = listAllCollections;

var _runAllTransactions = function(db,txDb,txCollection,onError,onSuccess){
  if(txCollection.length === 0){
    onSuccess();
    return;
  }

  //process and recur
  transaction.runTransaction(
    db,
    txDb,
    txCollection[0],
    onError,
    function(){
      _runAllTransactions(db,txDb,_.rest(txCollection), onError, onSuccess);
  })
};

//probably needs to return a deferred and implement accordingly
var commitAllLocals = function(onError,onSuccess){
  var toAddCount = LocalDb.Data.length;
  var added = 0;
  var errored = 0;
  var errorCollection = [];
  var insertedCollection = [];

  //create snapshot
  createNewSnapshot(
    dbName,
    //success callback only, doesn't require error callback
    function(snapshot,schemaDoc){

      //sort earliest first
      var _sorted = _.sortBy(
        LocalDb.Transactions,
        function(tx){
          return dbUtil.getDocTimestamp(tx);
      });

      _.print('sorted transactions:');
      _.print(JSON.stringify(_sorted));

      _runAllTransactions(
        snapshot,
        snapshot,
        _sorted,
        //onError
        function(err){
          //destroy snapshot
          destroySnapshot(
            snapshot,
            function(err){
              if(err) errorCollection.push({error:err});
              onError(errorCollection);
            });
        },
        //all transactions successfully completed
        function(){
          _.print('(transactor) committing snapshot');

          commitSnapshot(
            snapshot,
            schemaDoc,
            onError,
            function(){
              _.print('(transactor) snapshot committed, updating local copy');

              //update local copy
              //now also requires updating db with information in transaction...
              _.each(LocalDb.Data,markCommitted);
              _.each(
                LocalDb.Transactions,
                function(tx){
                  transaction.runTransactionAgainstLocal(tx,LocalDb.Data);
              });

              _.print('(transactor) listing all local post-snapshot');
              _.each(LocalDb.Data,function(doc){
                _.print(JSON.stringify(doc));
              });

              //update local transactions
              LocalDb.Transactions = [];

              onSuccess(LocalDb.Data);
          });
      });
  });
};
exports.commit = commitAllLocals;

var commitSnapshot = function(snapshot,schemaDoc,onError,onSuccess){

  schemaDoc._meta.isCommitted = true;

  snapshot.save(
    schemaDoc,
    function(err,result){
      if(err){
        _.print('(transactor) error committing snapshot, aborting');
        _.print('(transactor) error: ' + err);
        //destroy snapshot
        destroySnapshot(
          snapshot,
          noOp);

        onError([{error:err}]);
        return;
      }

      _.print('(transactor) snapshot committed, updating local copy');
      //update local copy
      _.each(LocalDb.Data,markCommitted);

      _.print('(transactor) listing all local post-snapshot');
      _.each(LocalDb.Data,function(doc){
        _.print(JSON.stringify(doc));
      });

      onSuccess(LocalDb.Data);
  });
};

var destroySnapshot = function(snapshot,onSuccess){
  db.dropCollection(snapshot.options.create,
    function(err){
    onSuccess();
  });
};

var _getLocalDbFromRemote = function(_db,callback){
  _db.find({}).toArray(
    function(err,docs){
      if(err) throw err;

      var toReturn = {
        Data: [],
        EventTransactions: [],
        Schema: {}
      };

      toReturn.Schema = schema.getSchemaDoc(docs);
      toReturn.Data = schema.getDataDocs(docs);
      toReturn.EventTransactions = transaction.getTransactionDocs(docs);

      callback(toReturn);
  });
};

var loadHead = function(_conn,_dbName,callback){

  var allSnaps = [];
  listAllSnapshots(_conn,_dbName,function(snaps){
    allSnaps = snaps;

    //load default
    if(allSnaps.length < 1){
      _.print('Loading "' + _dbName + '-Base"');

      getBaseName(
        _dbName,
        function(_baseName){
          _.print('Base name is "' + _baseName + '"');

          var _db = _conn.collection(_baseName);
          RemoteDb = _db;

          _getLocalDbFromRemote(_db,function(toReturn){
              LocalDb = toReturn;
              callback(toReturn);
          });
        });
    //load latest snapshot
    }else{

      var _sorted = _.sortBy(allSnaps,
        function(_snap){
          var _name = _snap.options.create.split('-')[1];
          return ObjectID(_name).getTimestamp().getTime() * -1;
        });

      var snapshotName = _sorted[0].options.create;

      _.print('Loading "' + snapshotName + '"');

      var _db = _conn.collection(snapshotName);
      RemoteDb = _db;

      _getLocalDbFromRemote(_db,function(toReturn){
          LocalDb = toReturn;
          callback(toReturn);
      });
    }
  });
};
exports.loadHead = loadHead;

var noOp = dbUtil.noOp;
exports.noOp = noOp;

var markCommitted = dbUtil.markCommitted;

var insertLocal = function(/*toInsert*/){
  var args = _.toArray(arguments);
  var tx = transaction.createTransaction.apply(this,args);

  LocalDb.Transactions.push(tx);

  return tx;
};

exports.insert = insertLocal;

var getUniqueId = function(){
  return dbUtil.getUniqueId();
};
exports.getUniqueId = getUniqueId;

var copyDoc = function(toCopy){
  return dbUtil.copyDoc(toCopy);
};

var listAllSnapshots = function(_conn,_dbName,callback){

  _conn.collectionNames(
    function(err, collections){
      if(err) throw err;

      var snapshots = _.filter(
        collections,
        function(item){
          return item.name.indexOf(_dbName + '-') >= 0 && //part of the db
            item.name.indexOf(_dbName + '-Base') === -1; //but not the base
        });

      _.each(snapshots,function(snap){
        _.print('Snapshot "' + snap.options.create + '"');
      });

      callback(snapshots);
    });
};
exports.listAllSnapshots = listAllSnapshots;

var getBaseName = function(name,onSuccess,_db){
  var _toRunDb = _db ? _db : db;

  _.print('Getting base for ' + name);

  _toRunDb.collectionNames(function(err, collections){

    var bases = _.filter(
      collections,
      function(item){
        return item.name.indexOf(name + '-Base') > -1;
      });

    var _noBaseError = 'No base exists for "' + name + '"';
    var _tooManyBasesError = 'More than one base exists for "' + name + '"';

    if(bases.length > 1){
      _.print(_tooManyBasesError);
      throw _tooManyBasesError;
    }
    if(bases.length < 1){
      _.print(_noBaseError);
      throw _noBaseError;
    }

    _.each(bases,function(base){
      _.print('Base "' + base.options.create + '"');
    });

    _.print('returning base name ' + bases[0].options.create);

    onSuccess(bases[0].options.create);
  });
};
exports.getBaseName = getBaseName;

var getSchemaDoc = function(){
  return schema.getSchemaDoc(LocalDb.Data);
};

var createNewSnapshot = function(name,onSuccess){
  var snapName = name + '-' + getUniqueId();
  _.print('Creating "' + snapName + '"');

  db.createCollection(
    snapName,
    function(err, collection){
      if (err) throw err;

      var schemaDoc = copyDoc(getSchemaDoc());
      schemaDoc._meta.isCommitted = true;

      collection.save(
        schemaDoc,
        function(err,doc){
          if(err) throw err;

          _.print('(transactor) listing all local pre-snapshot');
          _.each(LocalDb.Data,function(doc){
            _.print(JSON.stringify(doc));
          });

          _.print('Created snapshot:');
          _.print(JSON.stringify(schemaDoc));

          onSuccess(collection, schemaDoc);

        });//save schema doc
  });//create collection
};

var deleteBase = function(name,onSuccess,_conn){
  getBaseName(
    name,
    function(_baseName){
      deleteCollection(_baseName,onSuccess,_conn);
    },
    _conn);
};
exports.deleteBase = deleteBase;

var deleteAllSnapshots = function(name,onSuccess,_conn){

  listAllSnapshots(
    _conn,
    name,
    function(snapshots){
      _.print('deleting snapshots ' + JSON.stringify(snapshots));
      deleteCollectionList(snapshots,onSuccess,_conn);
    });

};
exports.deleteAllSnapshots = deleteAllSnapshots;

var deleteEverything = function(name,onSuccess,_conn){
  _.print('deleting everything');
  deleteAllSnapshots(
    name,
    function(){
      deleteBase(name,onSuccess,_conn);
    },
    _conn);
};
exports.deleteEverything = deleteEverything;

var deleteBeginningWith = function(name,onSuccess){
  db.collectionNames(function(err, collections){

    var _colls = _.filter(
      collections,
      function(item){
        return item.options && item.options.create &&
          item.options.create.indexOf(name) === 0;
      });

    deleteCollectionList(_colls,onSuccess);
  });
};
exports.deleteBeginningWith = deleteBeginningWith;

var deleteCollectionList = function(_colls,onSuccess,_conn){
  if(_colls.length < 1){
    onSuccess();
    return;
  }

  var _toRunDb = _conn || db;  
  var countToDelete = _colls.length;
  var deleted = 0;

  _.each(_colls,function(_coll){

    _.print('Deleting collection "' + _coll.options.create + '"');

    _toRunDb.dropCollection(_coll.options.create, function(err){
      if(err) throw err;
      deleted++;

      if(deleted === countToDelete) onSuccess();
    });
  });  
};