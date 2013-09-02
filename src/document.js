var getUniqueId = require('./dbUtil').getUniqueId;

var getNewDbDoc = function(toInsert){
  toInsert._meta = {
    type: 'Document',
    dbStatus: 'uncommitted',
    transactions: []
  };

  toInsert._id = getUniqueId();

  return toInsert;
};
exports.getNewDbDoc = getNewDbDoc;