var util = require('../../src/dbUtil.js')
  , peer = require('../../src/peer.js')
  , getSchemaDoc = require('../../src/schema.js').getSchemaDoc
  , transaction = require('../../src/transaction.js')
  , _ = require('underscore') 
  , help = require('../helpers');

var url = help.testUrl
  , runWithTestDb = help.runWithTestDb
  , createTransaction = transaction.createTransaction;

require('../../src/_extensions');
/*
db looks like this
var LocalDb = {
  Data: [],
  EventTransactions: [],
  Schema: {}
};
*/
describe('peer operations',function(){

  it('should load the schema doc in-memory',function(done){
    var _dbName = ':Peer:InMemorySchemaTest';

    runWithTestDb({
        done: done,
        url: url,
        dbName: _dbName
      },
      function(conn,cleanup){
        _dbName = 'AutoTest' + _dbName;
        //create the database
        peer.loadHead(conn,_dbName,function(db){

          //grab the schema doc
          var schema = db.Schema;
          expect(schema).toBeDefined();
          //assert some properties of it
          expect(schema._meta.type).toEqual('Schema');
          expect(schema._meta.isCommitted).toBe(true);
          expect(schema.DbName).toEqual('AutoTest:Peer:InMemorySchemaTest');
          expect(_.isEmpty(db.EventTransactions)).toBe(true);
          expect(_.isEmpty(db.Data)).toBe(true);
          cleanup();
        });       
    });    
  });

  it('should not affect the durable database if transactions are run locally',function(done){
    var _dbName = ':Peer:InMemoryOnlyTest';

    _.print('starting test');

    runWithTestDb({
        done: done,
        url: url,
        dbName: _dbName
      },
      function(conn,cleanup){
        _dbName = 'AutoTest' + _dbName;
        //create the database
        peer.loadHead(conn,_dbName,function(db){

          peer.runTxLocal(db,createTransaction({ test: 'doc1' },{ test: 'doc2' }));

          //assert some properties of it
          expect(_.size(db.EventTransactions)).toBe(1);
          expect(_.size(db.Data)).toBe(2);

          _.print('doing compare of head');
          peer.loadHead(conn,_dbName,function(newHead){
            //assert some properties of it
            expect(_.size(newHead.EventTransactions)).toBe(0);
            expect(_.size(newHead.Data)).toBe(0);

            cleanup();            
          });
        });       
    });
  });

});