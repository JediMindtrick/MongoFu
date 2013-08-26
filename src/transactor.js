var MongoClient = require('mongodb').MongoClient
 , _ = require('underscore')
 , ObjectID = require('mongodb').ObjectID
 , dbUtil = require('./dbUtil')
 , transaction = require('./transaction');

// URL for MongoDB
var url = '';
var db = null;
var RemoteDb = null;
//var LocalDb = [];
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
  console.log('Connecting to MongoDB...');
  setUrl(_url);

  MongoClient.connect(url, function (err, database) {
    if (err) throw err;
    db = database;

    console.log('Connected to MongoDB');

    onSuccess();
  });
};
exports.connect = connect;

var getRemote = function(){
  return RemoteDb;
};
exports.getRemote = getRemote;

var initializeCollection = function(collName,onSuccess){

  db.createCollection(collName, function(err, collection){
    if (err){
      console.log('problem creating collection ' + collName);
      throw err;
    }
    console.log('collection created ' + collName);
    RemoteDb = collection;

    var _schemaDoc = { 
      _meta: { 
        type: 'CollectionSchema',
        isCommitted: true
      }, 
      _id: getUniqueId() 
    };

    RemoteDb.save(
      _schemaDoc, 
      function(err,doc){
        if(err) throw err;        
        console.log('successfully initialized ' + collName);
        onSuccess(_schemaDoc);
    });
  });
};
exports.initializeCollection = initializeCollection;

var deleteCollection = function(collName,onSuccess){
    db.dropCollection(collName, function(err){
    if(err) throw err;        
    onSuccess();
  });  
};
exports.deleteCollection = deleteCollection;

var connectToCollection = function(_url,collName,onSuccess){
  setCollection(collName);

  console.log('Loading data for ' + dbName);

  connect(
    _url,
    function(){
      getHead(function(){
        console.log('Data loaded.');
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

var close = function(){
  console.log('closing...');
  db.close();
  console.log('closed');
};

exports.close = close;

var listAllCollections = function(onSuccess){
  db.collectionNames(function(err, collections){
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

      console.log('sorted transactions:');
      console.log(JSON.stringify(_sorted));

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
        //onSuccess,
        //all transactions successfully completed
        function(){
          console.log('(transactor) committing snapshot');

          commitSnapshot(
            snapshot,
            schemaDoc,
            onError,
            function(){
              console.log('(transactor) snapshot committed, updating local copy');

              //update local copy
              //now also requires updating db with information in transaction...
              _.each(LocalDb.Data,markCommitted);
              _.each(
                LocalDb.Transactions,
                function(tx){
                  transaction.runTransactionAgainstLocal(tx,LocalDb.Data);
              });

              console.log('(transactor) listing all local post-snapshot');
              _.each(LocalDb.Data,function(doc){
                console.log(JSON.stringify(doc));
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
        console.log('(transactor) error committing snapshot, aborting');
        console.log('(transactor) error: ' + err);
        //destroy snapshot
        destroySnapshot(
          snapshot,
          noOp);

        onError([{error:err}]);
        return;
      }

      console.log('(transactor) snapshot committed, updating local copy');
      //update local copy
      _.each(LocalDb.Data,markCommitted);

      console.log('(transactor) listing all local post-snapshot');
      _.each(LocalDb.Data,function(doc){
        console.log(JSON.stringify(doc));
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

//no need to update to head, but need to write a routine that will do so
var getHead = function(onSuccess){

  var allSnaps = [];
  listAllSnapshots(dbName,function(snaps){
    allSnaps = snaps;

    //load default
    if(allSnaps.length < 1){
      console.log('Loading "' + dbName + '-Base"');

      getBaseName(
        dbName,
        function(_baseName){
          console.log('Base name is "' + _baseName + '"');

          var collection = db.collection(_baseName);
          RemoteDb = collection;

          RemoteDb.find({}).toArray(
            function(err,docs){
              if(err) throw err;

              LocalDb.Data = docs;
              onSuccess(LocalDb.Data);
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

      console.log('Loading "' + snapshotName + '"');

      var collection = db.collection(snapshotName);
      RemoteDb = collection;

      RemoteDb.find({}).toArray(
        function(err,docs){
          if(err) throw err;
          LocalDb.Data = docs;
          onSuccess(LocalDb.Data);
      });      

    }
  });
};

var noOp = dbUtil.noOp;
exports.noOp = noOp;

var markCommitted = dbUtil.markCommitted;

//needs to be replaced or wrap runTransaction()
var upsertToSnapshot = function(snapshot,toInsert,onError,onSuccess){
  var newInsert = copyDoc(toInsert);

  newInsert = markCommitted(newInsert);

  snapshot.save( 
    newInsert,
    function(err,doc){
      if(err) onError(err);

      onSuccess(toInsert);
  });
};

var upsertToRemote = function(toInsert,onError,onSuccess){
  dbUtil.upsertToRemote(RemoteDb,toInsert,onError,onSuccess);
};

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

var listAllSnapshots = function(name,onSuccess){

  db.collectionNames(
    function(err, collections){    
      if(err) throw err;

      var snapshots = _.filter(
        collections,
        function(item){
          return item.name.indexOf(name + '-') >= 0 &&
            item.name.indexOf(name + '-Base') === -1; 
        });

      _.each(snapshots,function(snap){
        console.log('Snapshot "' + snap.options.create + '"');
      });

      onSuccess(snapshots);      
    });
};
exports.listAllSnapshots = listAllSnapshots;

var getBaseName = function(name,onSuccess){

  db.collectionNames(function(err, collections){    

    var bases = _.filter(
      collections,
      function(item){ 
        return item.name.indexOf(name + '-Base') > -1; 
      });

    var _noBaseError = 'No base exists for "' + name + '"';
    var _tooManyBasesError = 'More than one base exists for "' + name + '"';

    if(bases.length > 1){
      console.log(_tooManyBasesError);
      throw _tooManyBasesError;
    } 
    if(bases.length < 1){
      console.log(_noBaseError);
      throw _noBaseError;
    }

    _.each(bases,function(base){
      console.log('Base "' + base.options.create + '"');
    });

    onSuccess(bases[0].options.create);
  });
};
exports.getBaseName = getBaseName;

var getSchemaDoc = function(){
  return _.find(LocalDb.Data,function(doc){
    return doc._meta.type === 'CollectionSchema';
  });
};

var createNewSnapshot = function(name,onSuccess){
  var snapName = name + '-' + getUniqueId();
  console.log('Creating "' + snapName + '"');

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

          console.log('(transactor) listing all local pre-snapshot');
          _.each(LocalDb.Data,function(doc){
            console.log(JSON.stringify(doc));
          });

          console.log('Created snapshot:');
          console.log(JSON.stringify(schemaDoc));

          onSuccess(collection, schemaDoc);    
          
        });//save schema doc
  });//create collection
};

var deleteAllSnapshots = function(name,onSuccess){
  db.collectionNames(function(err, collections){    
    listAllSnapshots(
      name,
      function(snapshots){

        var countToDelete = snapshots.length;
        var deleted = 0;

        _.each(snapshots,function(_coll){

          console.log('Deleting snapshot "' + _coll.options.create + '"');

          db.dropCollection(_coll.options.create, function(err){
            if(err) throw err;
            deleted++;

            if(deleted === countToDelete) onSuccess();
          });

        });
      });
  });  
};
exports.deleteAllSnapshots = deleteAllSnapshots;