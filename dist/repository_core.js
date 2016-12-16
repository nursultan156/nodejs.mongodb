/**
 * Created by n on 12.12.2016.
 */
var GridFSBucket = require('mongodb').GridFSBucket;
var fs = require('fs');

var repository_core = function (connector) {

    var self = this;
    var _connector = connector;


    //payload extracts
    /**
     * _collectionName: string
     */
    var _collectionName = null; //CRUD [CRUD] COUNT AGGREGATE CREATE-INDEX
    /**
     * _document: {}
     */
    var _document = null;       //C--- [----] ----- --------- CREATE-INDEX
    /**
     * _query: {}
     */
    var _query = null;          //-RUD [-RUD] COUNT --------- ------------
    /**
     * _update: {}
     */
    var _update = null;         //--U- [--U-] ----- --------- ------------
    /**
     * _options: {}
     */
    var _options = null;        //C-UD [C-UD] COUNT AGGREGATE CREATE-INDEX
    /**
     * _documentsList: []
     */
    var _documentsList = null;  //---- [C---] ----- --------- ------------
    /**
     * _cursorOptions: {
     *      project: {},
     *      skip: number,
     *      limit: number,
     *      sort: {},
     *      toArray: bool
     * }
     */
    var _cursorOptions = null;  //---- [-R--] ----- AGGREGATE ------------
    /**
     * _pipeline: []
     */
    var _pipeline = null;       //---- [----] ----- AGGREGATE ------------
    //var _requestId = null;    //---- [----] ----- --------- ------------


    //private functions
    var reconnect = function (func, payload, callback) {
        _connector.reconnect(function (err, res) {
            if (err) return callback(err, null);
            func(payload, callback);
        });
    };
    var payloadExtract = function (payload) {
        _collectionName = payload && payload.collectionName ? payload.collectionName : null;
        _document = payload && payload.document ? payload.document : null;
        _query = payload && payload.query ? payload.query : null;
        _update = payload && payload.update ? payload.update : null;
        _options = payload && payload.options ? payload.options : null;
        _documentsList = payload && payload.documentsList ? payload.documentsList : null;
        _cursorOptions = payload && payload.cursorOptions ? payload.cursorOptions : null;
        _pipeline = payload && payload.pipeline ? payload.pipeline : null;
        //_requestId = payload && (payload.requestId || payload.requestId == 0) ? payload.requestId : null;
        //_filter = payload && payload.filter ? payload.filter : null;
    };
    var isDbConnectionProblem = function (err) {
        if (err && err.message && err.message.indexOf('topology was destroyed') != -1) {
            return true;
        }
        else if (err && err.message && err.message.indexOf('ECONNRESET') != -1) {
            return true;
        }
        return false;
    };


    //private functions
    var createOne = function (payload, callback) {

        payloadExtract(payload);

        if (!_collectionName || !_document) return callback('payload error', null);

        if (_connector.isConnected()) {

            var collection = _connector.db().collection(_collectionName);
            collection.insertOne(_document, _options, function (err, res) {
                if (err) {
                    if (isDbConnectionProblem(err)) {
                        reconnect(createOne, payload, callback);
                    }
                    else {
                        callback(err, null);
                    }
                }
                else {
                    callback(null, res);
                }
            });
        }
        else {
            reconnect(createOne, payload, callback);
        }
    };
    var readOne = function (payload, callback) {

        payloadExtract(payload);

        if (!_collectionName) return callback('payload error', null);

        if (_connector.isConnected()) {

            var collection = _connector.db().collection(_collectionName);
            collection.find(_query).limit(1).next(function (err, doc) {
                if (err) {
                    if (isDbConnectionProblem(err)) {
                        reconnect(readOne, payload, callback);
                    }
                    else {
                        callback(err, null);
                    }
                }
                else {
                    callback(null, doc);
                }
            });
        }
        else {
            reconnect(readOne, payload, callback);
        }
    };
    var updateOne = function (payload, callback) {

        payloadExtract(payload);

        if (!_collectionName) return callback('payload error', null);

        if (_connector.isConnected()) {

            var collection = _connector.db().collection(_collectionName);
            collection.updateOne(_query, _update, _options, function (err, res) {
                if (err) {
                    if (isDbConnectionProblem(err)) {
                        reconnect(updateOne, payload, callback);
                    }
                    else {
                        callback(err, null);
                    }
                }
                else {
                    callback(null, res);
                }
            });
        }
        else {
            reconnect(updateOne, payload, callback);
        }

    };
    var deleteOne = function (payload, callback) {

        payloadExtract(payload);

        if (!_collectionName) return callback('payload error', null);

        if (_connector.isConnected()) {

            var collection = _connector.db().collection(_collectionName);
            collection.updateOne(_query, _update, _options, function (err, res) {
                if (err) {
                    if (isDbConnectionProblem(err)) {
                        reconnect(deleteOne, payload, callback);
                    }
                    else {
                        callback(err, null);
                    }
                }
                else {
                    callback(null, res);
                }
            });
        }
        else {
            reconnect(deleteOne, payload, callback);
        }

    };
    var createMany = function (payload, callback) {

        payloadExtract(payload);

        if (!_collectionName || !_documentsList) return callback('payload error', null);

        if (_connector.isConnected()) {

            var collection = _connector.db().collection(_collectionName);
            collection.insertMany(_documentsList, _options, function (err, res) {
                if (err) {
                    if (isDbConnectionProblem(err)) {
                        reconnect(createMany, payload, callback);
                    }
                    else {
                        callback(err, null);
                    }
                }
                else {
                    callback(null, res);
                }
            });
        }
        else {
            reconnect(createMany, payload, callback);
        }

    };
    var readMany = function (payload, callback) {

        payloadExtract(payload);

        if (!_collectionName) return callback('payload error', null);

        if (_connector.isConnected()) {

            var collection = _connector.db().collection(_collectionName);
            var cursor = collection.find(_query);
            if (_cursorOptions && _cursorOptions.project) cursor = cursor.project(_cursorOptions.project);
            if (_cursorOptions && _cursorOptions.skip) cursor = cursor.skip(_cursorOptions.skip);
            if (_cursorOptions && _cursorOptions.limit) cursor = cursor.limit(_cursorOptions.limit);
            if (_cursorOptions && _cursorOptions.sort) cursor = cursor.sort(_cursorOptions.sort);
            if (_cursorOptions && _cursorOptions.toArray) {
                cursor.toArray(function (err, docs) {
                    if (err) {
                        if (isDbConnectionProblem(err)) {
                            reconnect(readMany, payload, callback);
                        }
                        else {
                            callback(err, null);
                        }
                    }
                    else {
                        callback(null, docs);
                    }
                });
            }
            else {
                callback(null, cursor);
            }

        }
        else {
            reconnect(readMany, payload, callback);
        }

    };
    var updateMany = function (payload, callback) {

        payloadExtract(payload);

        if (!_collectionName) return callback('payload error', null);

        if (_connector.isConnected()) {

            var collection = _connector.db().collection(_collectionName);
            collection.updateMany(_query, _update, _options, function (err, res) {
                if (err) {
                    if (isDbConnectionProblem(err)) {
                        reconnect(updateMany, payload, callback);
                    }
                    else {
                        callback(err, null);
                    }
                }
                else {
                    callback(null, res);
                }
            });
        }
        else {
            reconnect(updateMany, payload, callback);
        }

    };
    var deleteMany = function (payload, callback) {

        payloadExtract(payload);

        if (!_collectionName) return callback('payload error', null);

        if (_connector.isConnected()) {

            var collection = _connector.db().collection(_collectionName);
            collection.updateMany(_query, _update, _options, function (err, res) {
                if (err) {
                    if (isDbConnectionProblem(err)) {
                        reconnect(deleteMany, payload, callback);
                    }
                    else {
                        callback(err, null);
                    }
                }
                else {
                    callback(null, res);
                }
            });
        }
        else {
            reconnect(deleteMany, payload, callback);
        }

    };
    var aggregate = function (payload, callback) {

        payloadExtract(payload);

        if (!_collectionName || !_pipeline) return callback('payload error', null);

        if (_connector.isConnected()) {

            var collection = _connector.db().collection(_collectionName);
            var cursor = collection.aggregate(_pipeline, _options);
            if (_cursorOptions && _cursorOptions.toArray) {
                cursor.toArray(function (err, docs) {
                    if (err) {
                        if (isDbConnectionProblem(err)) {
                            reconnect(aggregate, payload, callback);
                        }
                        else {
                            callback(err, null);
                        }
                    }
                    else {
                        callback(null, docs);
                    }
                });
            }
            else {
                callback(null, cursor);
            }

        }
        else {
            reconnect(aggregate, payload, callback);
        }

    };
    var count = function (payload, callback) {

        payloadExtract(payload);

        if (!_collectionName) return callback('payload error', null);

        if (_connector.isConnected()) {

            var collection = _connector.db().collection(_collectionName);
            collection.count(_query, _options, function (err, res) {
                if (err) {
                    if (isDbConnectionProblem(err)) {
                        reconnect(count, payload, callback);
                    }
                    else {
                        callback(err, null);
                    }
                }
                else {
                    callback(null, res);
                }
            });
        }
        else {
            reconnect(count, payload, callback);
        }

    };
    var createIndex = function (payload, callback) {

        payloadExtract(payload);

        if (!_collectionName || !_document) return callback('payload error', null);

        if (_connector.isConnected()) {

            var collection = _connector.db().collection(_collectionName);
            collection.createIndex(_document, _options, function (err, res) {
                if (err) {
                    if (isDbConnectionProblem(err)) {
                        reconnect(createIndex, payload, callback);
                    }
                    else {
                        callback(err, null);
                    }
                }
                else {
                    callback(null, res);
                }
            });
        }
        else {
            reconnect(createIndex, payload, callback);
        }

    };


    var uploadFile = function (payload, callback) {

        /**
         * payload = {
         *  document:{
         *      fileName:fileName
         *      fileOptions:fileOptions
         *      filePath:filePath
         *  }
         *  options:options
         * }
         */

        payloadExtract(payload);

        if (!_document || !_document.filePath || !fs.existsSync(_document.filePath)) return callback('payload error', null);

        if (_connector.isConnected()) {

            var bucket = new GridFSBucket(_connector.db());
            var uploadStream = bucket.openUploadStream(_document && _document.fileName ? _document.fileName : null, _document && _document.fileOptions ? _document.fileOptions : null);
            var readStream = fs.createReadStream(_document.filePath);

            readStream.pipe(uploadStream)
                .on('error', function (err) {
                    if (isDbConnectionProblem(err)) {
                        reconnect(uploadFile, payload, callback);
                    }
                    else {
                        callback(err, null);
                    }
                })
                .on('finish', function () {
                    callback(null, uploadStream.id);
                });

        }
        else {
            reconnect(uploadFile, payload, callback);
        }

    };
    var downloadFile = function (payload, callback) {

        /**
         * payload = {
         *  query:query
         *  options:options
         * }
         */

    };


    //public functions
    this.createOne = function (payload, callback) {
        if (payload && payload.document && !payload.document._id) {
            if (!payload.options) {
                payload.options = {};
            }
            payload.options.forceServerObjectId = true;
        }
        createOne(payload, callback);
    };
    this.readOne = function (payload, callback) {
        readOne(payload, callback);
    };
    this.updateOne = function (payload, callback) {
        updateOne(payload, callback);
    };
    this.deleteOne = function (payload, callback) {
        if (payload) {
            payload.update = {};
            payload.update.$set = {};
            payload.update.$set.isDeleted = true;
        }
        deleteOne(payload, callback);
    };
    this.createMany = function (payload, callback) {
        createMany(payload, callback);
    };
    this.readMany = function (payload, callback) {
        if (payload) {
            if (!payload.cursorOptions) {
                payload.cursorOptions = {};
            }
            if (!payload.cursorOptions.limit && payload.cursorOptions.limit != 0 && payload.cursorOptions.toArray) {
                payload.cursorOptions.limit = 100;
            }
        }
        readMany(payload, callback);
    };
    this.updateMany = function (payload, callback) {
        updateMany(payload, callback);
    };
    this.deleteMany = function (payload, callback) {
        if (payload) {
            payload.update = {};
            payload.update.$set = {};
            payload.update.$set.isDeleted = true;
        }
        deleteMany(payload, callback);
    };
    this.aggregate = function (payload, callback) {
        if (payload) {
            if (!payload.options) {
                payload.options = {};
            }
            if (!payload.options.cursor) {
                payload.options.cursor = {};
            }
        }
        aggregate(payload, callback);
    };
    this.count = function (payload, callback) {
        count(payload, callback);
    };
    this.createIndex = function (payload, callback) {
        createIndex(payload, callback);
    };

};

module.exports = repository_core;


























