var transactor = require('../src/transactor.js')
  , _ = require('underscore');

require('../src/_extensions');

exports.testUrl = 'mongodb://localhost:27017/mongtomic';

/*Here's an example call:
    runWithTestDb({
        done: done,
        url: url,
        dbName: ':InMemoryTest'
      },
      function(conn,cleanup){


        cleanup();
    });
*/
exports.runWithTestDb = function(opts,toRun){

  var _dbName = 'AutoTest' + opts.dbName;

  transactor.connect(
    opts.url,
    function(conn){

      //this actually creates our database
      transactor.initializeBase(
        _dbName,
        function(_schemaDoc){

          try{

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

          //otherwise, jasmine will just continue executing, giving no results, not reporting
          //the exception and just silently finishing
          }catch(err){
            expect('Uncaught exception while running test: ' + err).toBeUndefined();
            _.print(err);
            conn.close();
            opts.done();
          }




        },
        conn);
    });
};