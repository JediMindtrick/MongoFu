var dbUtil = require('../../src/dbUtil.js')
  , schema = require('../../src/schema.js')
  , _ = require('underscore');

var getSchemaDoc = schema.getNewSchemaDoc
	, getConstraintChecker = schema.getConstraintChecker;

require('../../src/_extensions');

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
    	var _schema = getSchemaDoc(':Schema:DefaultDocumentConstraintTest');
    	var _checker = getConstraintChecker(_schema);
    	var _toCheck = dbUtil.getNewDbDoc({});
    	var _mockDb = {
    		Schema: _schema
    	};

    	expect(_checker.Document.atAllTimes(_mockDb,[],_toCheck)).toBe(true);
    	_toCheck._meta.type = 'foo';
    	expect(_checker.Document.atAllTimes(_mockDb,[],_toCheck)).toBe(false);
    });
});