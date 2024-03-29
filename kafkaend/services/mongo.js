var MongoClient = require('mongodb').MongoClient;
var mongoURL = require('./config').dbUrl;

var db;
var connected = false;


/**
 * Connects to the MongoDB Database with the provided URL
 */
var connect = function(){
  MongoClient.connect(mongoURL, function(err, _db){
    if (err) { throw new Error('Could not connect: '+err); }
    db = _db;
    connected = true;
  });
};

connect();

/**
 * Returns the collection on the selected database
 */
var getCollection = function(name,callback){
  if (!connected) {
    throw new Error('Must connect to Mongo before calling "collection"');
  } else {
    callback(null,db.collection(name));
  }
};

var getDb = function(){
  return db;
}

exports.getCollection = getCollection;
exports.getDb = getDb;