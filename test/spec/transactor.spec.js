var transactor = require('../../src/transactor.js')
  , _ = require('underscore')
  , url = require('../helpers').testUrl;
require('../../src/_extensions');


describe('database-level operations',function(){


  it('should create and destroy databases',function(done){
    expect(true).toBe(true);

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
        dbName: ':BaseCreateTest'
      },
      function(conn,cleanup){

        transactor.listAllCollections(
          function(collections){

            expect(
              _.some(collections,function(coll){
                return coll.name.indexOf('AutoTest:BaseCreateTest-Base') > -1;
                })
              ).toBe(true);

            cleanup();    
          }
          ,conn);        
    });
  });

  it('should load an in-memory database',function(done){
    runWithTestDb({
        done: done,
        url: url,
        dbName: ':InMemoryTest'
      },
      function(conn,cleanup){


        cleanup();
    });
  });

});

var runWithTestDb = function(opts,toRun){

  var _dbName = 'AutoTest' + opts.dbName;

  transactor.connect(
    opts.url,
    function(conn){

      //this actually creates our database
      transactor.initializeBase(
        _dbName,
        function(_schemaDoc){

          //callback will be called by test code
          //toRun is our actual test
          toRun(conn,function(){

            transactor.deleteEverything(
              _dbName,
              function(){
                conn.close();
                opts.done();
              },
              conn);
          });
        },
        conn);
    });
};