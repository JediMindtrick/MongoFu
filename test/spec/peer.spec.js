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
    runWithTestDb({
        done: done,
        url: url,
        dbName: ':Peer:InMemoryTest'
      },
      function(conn,cleanup){

/*
        //create the database
        peer.loadHead(conn,dbName,function(db){
          //grab the schema doc
          var schema = db.Schema;
          //assert some properties of it
          expect(schema._meta.type).toEqual('Schema');
          expect(schema._meta.isCommitted).toBe(true);
          expect(schema.DbName).toEqual('AutoTest:Peer:InMemoryTest');
        });
        */


        cleanup();
    });
  });

});