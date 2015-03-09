var mongo = require('mongodb');

/**
 * @namespace
 */
mg = {};

/**
 * @typeDef {function(!mongo.Collection)}
 */
mg.CollectionOperation;


/**
 * @typeDef {function}
 */
mg.CompleteHandler;


/**
 * @typeDef {function}
 */
mg.ErrorHandler;


/**
 * @type {string}
 */
mg.URL = '';


/**
 * @param {number} port
 * @param {string} opt_host
 */
mg.init = function(port, opt_host) {
  mg.URL = 'mongodb://' + opt_host || '0.0.0.0' + ':' + port + '/test';
};


/**
 * @param {!mg.CompleteHandler} complete
 * @param {!mg.ErrorHandler} cancel
 * @return {function(?Object, Object)}
 */
mg.__resultHandler = function(complete, cancel) {
  return function(err, result) {
    if (err) {
      cancel(err);
    } else {
      complete(result);
    }
  };
};


/**
 * @param {string} collectionName
 * @param {!mg.CollectionOperation} operation
 */
mg.operation = function(collectionName, operation) {

  /**
   * @param {mongo.Db} mongoDb
   */
  function task(mongoDb) {
    var collection = mongoDb.collection(collectionName);
    operation(collection);
  }

  mg.__queue.push(task);
  mg.__flush();
};


/**
 * send all requests
 */
mg.__flush = function() {
  mongo.MongoClient.connect(mg.URL, function(err, db) {
    if(err) {
      console.error(err);
    } else {
      while (mg.__queue.length > 0) {
        mg.__queue.shift()(db);
      }
      db.close();
    }
  })
};


/**
 * @type {Array}
 */
mg.__queue = [];


/**
 * @param {string} collectionName
 * @param {!Object} document
 * @param {!mg.CompleteHandler} complete
 * @param {!mg.ErrorHandler} cancel
 */
mg.addDocument = function(collectionName, document, complete, cancel) {
  mg.operation(collectionName, function(collection) {
    collection.insertOne(document, mg.__resultHandler(complete, cancel));
  })
};


mg.addDocuments = function(collectionName, documents, complete, cancel) {
  mg.operation(collectionName, function(collection) {
    collection.insertMany(documents, mg.__resultHandler(complete, cancel));
  })
};


mg.set = function(collectionName, query, key, value, complete, cancel) {
  mg.operation(collectionName, function(collection) {
    collection.updateOne(query, {$set: { key : value }}, mg.__resultHandler(complete, cancel));
  })
};


mg.setAll = function(collectionName, query, key, value, complete, cancel) {
  mg.operation(collectionName, function(collection) {
    collection.updateMany(query, {$set: { key : value }}, mg.__resultHandler(complete, cancel));
  })
};


mg.remove = function(collectionName, query, key, value, complete, cancel) {
  mg.operation(collectionName, function(collection) {
    collection.deleteOne(query, {$set: { key : value }}, mg.__resultHandler(complete, cancel));
  })
};


mg.removeAll = function(collectionName, query, key, value, complete, cancel) {
  mg.operation(collectionName, function(collection) {
    collection.deleteMany(query, {$set: { key : value }}, mg.__resultHandler(complete, cancel));
  })
};

