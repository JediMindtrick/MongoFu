var _ = require('underscore');

var noOp = function(){};
var always = function(){ return true; };
var safeInvoke = function(/*args*/){  
	var args = _.toArray(arguments);
	if(_.head(args))
		args[0].apply(this,_.rest(args));
};
var print = function(msg){
	console.log(msg);
};

_.mixin({
	noOp: noOp,
	always: always,
	print: print,
	safeInvoke: safeInvoke
});