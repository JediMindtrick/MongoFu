var util = require('../../src/dbUtil.js')
  , transactor = require('../../src/transactor.js')
  , _ = require('underscore') 
  , help = require('../helpers');

var url = help.testUrl
  , runWithTestDb = help.runWithTestDb;

require('../../src/_extensions');

describe('transactor operations',function(){

  it('should create and destroy databases',function(done){

    transactor.connect(
      url,
      function(conn){

        var _dbName = 'AutoTest' + transactor.getUniqueId();

        transactor.initializeBase(
          _dbName,
          function(_schemaDoc){

            transactor.getBaseName(
              _dbName,
              
              function(name){

                expect(name).toMatch(_dbName);

                transactor.deleteCollection(name,function(){
                  conn.close();
                  done();                                  
                },conn);
              },

              conn);
          },conn);
      });

  });
  
  it('should create a database with the name "Base"',function(done){

    runWithTestDb({
        done: done,
        url: url,
        dbName: ':Transactor:BaseCreateTest'
      },
      function(conn,cleanup){

        transactor.listAllCollections(
          function(collections){

            expect(
              _.some(collections,function(coll){
                return coll.name.indexOf('AutoTest:Transactor:BaseCreateTest-Base') > -1;
                })
              ).toBe(true);

            cleanup();    
          }
          ,conn);        
    });

  });

//  it('should run transactions against a durable store')
//  it('should create durable snapshots')

});