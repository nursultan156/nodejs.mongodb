/**
 * Created by vaio on 10.12.2016.
 */
var MongoClient = require('mongodb').MongoClient;
var count = 0;

var connector = function () {

    var self = this;

    var _url = null;
    var _options = null;

    var _db = null;
    var _isConnecting = false;
    var _isConnected = false;

    this.db = function () {
        return _db;
    };

    this.isConnected = function () {
        return _isConnected;
    };

    this.connect = function (callback) {
        if (!_isConnecting) {
            _isConnecting = true;

            MongoClient.connect(_url, _options, function (err, db) {
                _isConnecting = false;
                if (err) return callback(err, null);
                _db = db;
                _isConnected = true;
                callback(null, db);
            });

        }
        else {
            callback('connect busy', null);
        }
    };

    this.reconnect = function (callback) {
        self.connect(callback);
    };

    this.setup = function (url, options) {
        _url = url;
        _options = options;
    };

    this._id = count;

};

module.exports.new = function (url, options) {
    count++;
    var inst = new connector();
    inst.setup(url, options);
    return inst;
};