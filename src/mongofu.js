var MongoClient = require('mongodb').MongoClient
 , _ = require('underscore')
 , ObjectID = require('mongodb').ObjectID
 , peer = require('./transactor')
 , transact = require('./transaction');
require('./_extensions.js');


//mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
var _remoteUrl = 'mongodb://CaseRMMongo:YajoaGyDA4Br_BkvfawyPVze!';
_remoteUrl += 'iCDkzmB9ytXOr0MXeE-@ds035747.mongolab.com:35747/CaseRMMongo';
var _localUrl = 'mongodb://localhost:27017/mongtomic';
var _runningRemote = false;
var url = _runningRemote ? _remoteUrl : _localUrl;

var db = null;
var RemoteDb = null;
var LocalDb = [];
var dbName = 'Test';

var arg1 = process.argv[2];
var arg2 = process.argv[3];
var arg3 = process.argv[4];

var noOp = peer.noOp;

var _getLatest = function(onSuccess){
  peer.connectToCollection(
  url,
  dbName,
  onSuccess);
};

var start = function(){

  //bypass getLatest
  if(arg1 !== '-t' || _.contains(['-listAllRemote','-loadSnapshot','-load'],arg2)){
    console.log('Bypassing default load');
    runCommand();

  }else{
    _getLatest(
      function(_local){
        LocalDb = _local;
        runCommand();
    });
  }
};

var runCommand = function(){
  if(arg1 === '-t' && tests[arg2]){
    tests[arg2](function(){
      peer.close();
      console.log('done');
    });
    return;
  }

  if(operations[arg1]){
    peer.connect(url,function(){
      operations[arg1](function(){
        peer.close();
        console.log('done');
      });
    });
    return;
  }else{
    throw 'unsupported operation "' + operation + '"';
  }
};

var tests = {
  '-testTransact':function(onSuccess){

    var tx = transact.createTransaction({});

    console.log(JSON.stringify(tx));

    var _db = peer.getRemote();
    var _txDb = _db;

    transact.runTransaction(_db,_txDb,tx,
      onSuccess,
      function(){
        tests['-listAllRemote'](onSuccess);
      });
  },
  '-loadSnapshot': function(onSuccess){
      peer.connect(
        url,
        dbName,
        function(){
          peer.getSnapshot(
            arg3,
            function(snap){
              LocalDb = snap;
              console.log('Snapshot data loaded.');
              tests['-listAllLocal'](onSuccess);
          });
        });
  },
  '-commitMany': function(onSuccess){
    var _id = peer.getUniqueId();
    var toInsert1 = peer.insert({msg:'-commitMany first ' + _id});
    var toInsert2 = peer.insert({msg:'-commitMany second ' + _id});


    console.log('(test) Listing all local:');
    tests['-listAllLocal'](
      function(){
        peer.commit(
          function(err){
            console.log(JSON.stringify(err));
            onSuccess();
          },
          function(newLocal){
            console.log('commit finished');

            LocalDb = newLocal;

            console.log('(test) Listing all local:');
            tests['-listAllLocal'](onSuccess);
          });
    });

  },
  '-insertLocalThenRemote': function(onSuccess){
    var toInsert = peer.insert({msg:'-insertLocalThenRemote'});

    console.log('Saved document to local:');
    console.log(JSON.stringify(toInsert));
    console.log('Listing all local:');
    tests['-listAllLocal'](noOp);

    peer.commit(
      function(err){
        console.log(JSON.stringify(err));
        onSuccess();
      },
      function(){
        console.log('Listing all remote:');
        tests['-listAllRemote'](onSuccess);
      });
  },
  '-insertLocalOnly': function(onSuccess){

    peer.insert({});

    console.log('Saved document to local:');
    console.log(JSON.stringify(toInsert));
    console.log('Listing all local:');
    tests['-listAllLocal'](noOp);
    console.log('Listing all remote:');
    tests['-listAllRemote'](onSuccess);
  },
  '-listAllLocal': function(onSuccess){
    console.log('listing all local');
    _.each(LocalDb,function(doc){
      console.log(JSON.stringify(doc));
    });
    onSuccess();
  },
  '-listAllRemote': function(onSuccess){

    _getLatest(function(docs){
      _.each(docs,function(doc){
        console.log(JSON.stringify(doc));
      });
      onSuccess();
    });
  }
};

var operations = {
  '-load': function(onSuccess){
    peer.connect(url,function(_conn){

      var _db = _conn.collection(arg2);
      _db.find({}).toArray(function(err,docs){

        _.print(docs);  
        _conn.close();
        onSuccess();
      });
    });
  },
  '-recreate': function(onSuccess){
    operations['-delete'](function(){
      operations['-init'](onSuccess);
    });
  },
  '-initBase':function(onSuccess){
      peer.initializeBase(
          arg2,
          function(_schemaDoc){
              console.log(JSON.stringify(_schemaDoc));
              onSuccess();
          });
  },
  '-deleteBase': function(onSuccess){
      console.log('attempting to delete base for ' + arg2);
      peer.getBaseName(arg2,function(name){
          peer.deleteCollection(name,onSuccess);
      });
  },
  '-deleteBeginningWith': function(onSuccess){
    peer.deleteBeginningWith(arg2,onSuccess);
  },
  '-la': function(onSuccess){
    peer.listAllCollections(function(collections){
      _.each(collections,function(coll){
        console.log('Collection "' + coll.name + '"');
      });
      onSuccess();
    });
  },
  '-d': function(onSuccess){
    console.log('preparing to delete snapshots...');
    peer.deleteAllSnapshots(dbName,function(coll){
      console.log('snapshots deleted');
      peer.listAllSnapshots(dbName,function(snapshots){
        console.log(snapshots);
        onSuccess();
      });
    });
  },
  '-l': function(onSuccess){
    peer.listAllSnapshots(dbName,function(snapshots){
      peer.getBaseName(dbName,function(name){
        onSuccess();
      });
    });
  }
};

start();