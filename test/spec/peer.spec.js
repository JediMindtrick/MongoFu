var util = require('../../src/dbUtil.js')
  , peer = require('../../src/peer.js')
  , getSchemaDoc = require('../../src/schema.js').getSchemaDoc
  , _ = require('underscore') 
  , help = require('../helpers');

var url = help.testUrl
  , runWithTestDb = help.runWithTestDb;

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
          expect(schema.DbName).toEqual('AutoTest:Peer:InMemoryTest');
          expect(_.isEmpty(schema.EventTransactions)).toBe(true);
          expect(_.isEmpty(schema.Data)).toBe(true);
          cleanup();
        });       
    });
  });

});