var dbUtil = require('../../src/dbUtil.js')
  , schema = require('../../src/schema.js')
  , getNewDbDoc = require('../../src/document.js').getNewDbDoc
  , createTransaction = require('../../src/transaction.js').createTransaction
  , _ = require('underscore');

var getSchemaDoc = schema.getNewSchemaDoc
	, getConstraintChecker = schema.getConstraintChecker;

require('../../src/_extensions');

var _schema = getSchemaDoc(':SchemaTests');
var _checker = getConstraintChecker(_schema);
var _mockDb = {
	Schema: _schema
};

describe('schema', function(){

    it('should convert a schema doc into a constraint checker', function(){

    	var _schema = getSchemaDoc(':Schema:ConstraintCheckerTest');
    	var _checker = getConstraintChecker(_schema);

    	expect(_checker.Document.atAllTimes).toBeDefined();
    	expect(_checker.Document.onUpsert).toBeDefined();
    	expect(_checker.Document.onDelete).toBeDefined();

    	expect(_checker.Transaction.atAllTimes).toBeDefined();
    	expect(_checker.Transaction.onUpsert).toBeDefined();
    	expect(_checker.Transaction.onDelete).toBeDefined();

    	expect(_checker.Schema.atAllTimes).toBeDefined();
    	expect(_checker.Schema.onUpsert).toBeDefined();
    	expect(_checker.Schema.onDelete).toBeDefined();
    });

    it('should provide a default invariant constraint for "Document" types',function(){
    	var _doc = getNewDbDoc({ test: 'doc1'});
    	var _toCheck = createTransaction(_doc);

    	_.print(_toCheck);

    	expect(_checker.Document.atAllTimes(_mockDb,_toCheck,_doc)).toBe(true);
    	_doc._meta.type = 'foo';
    	expect(_checker.Document.atAllTimes(_mockDb,[],_toCheck)).toBe(false);
    });

    it('should provide a function to check all constraints and report back failures',function(){
    	var _doc = getNewDbDoc({ test: 'doc1'});
    	var _toCheck = createTransaction(_doc);

    	expect(_checker.checkTxConstraints(_mockDb,_toCheck).length).toBe(0);
    	_doc._meta.type = 'foo';
    	expect(_checker.checkTxConstraints(_mockDb,_toCheck).length).toBe(1);
    	var _err = 'foo [' + _doc._id + '] has no registered contraints in schema.  Must at least have all "always" constraints.';
    	expect(_checker.checkTxConstraints(_mockDb,_toCheck)).toContain(_err);

    	_doc._meta.lastTransaction = '';			
    	expect(_checker.checkTxConstraints(_mockDb,_toCheck).length).toBe(1);
    	_err = 'foo [' + _doc._id + '] has no registered contraints in schema.  Must at least have all "always" constraints.';
    	expect(_checker.checkTxConstraints(_mockDb,_toCheck)).toContain(_err); 
    });
    
    it('should require every Document change to be associated with a transaction',function(){
    	var _doc = getNewDbDoc({ test: 'doc1'});
    	var _toCheck = createTransaction(_doc);

    	_doc._meta.lastTransaction = '';			
    	expect(_checker.checkTxConstraints(_mockDb,_toCheck).length).toBe(1);
    	var _err = 'Document [' + _doc._id + '] failed constraint: atAllTimes.';
    	expect(_checker.checkTxConstraints(_mockDb,_toCheck)).toContain(_err); 

    	_doc._meta.lastTransaction = _toCheck._id;
    	expect(_checker.checkTxConstraints(_mockDb,_toCheck).length).toBe(0);

    	_doc._meta.transactions = [];
    	_err = 'Document [' + _doc._id + '] failed constraint: atAllTimes.';
    	expect(_checker.checkTxConstraints(_mockDb,_toCheck)).toContain(_err); 

    });
});