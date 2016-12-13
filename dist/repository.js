/**
 * Created by vaio on 10.12.2016.
 */
var connector = require('./connector');
var repository_core = require('./repository_core');
var count = 0;

var repository = function(url, options){

    var _connector = connector.new(url, options);

    this._id = count;

    this.createOne = function(payload, callback){
        var inst = new repository_core(_connector);
        inst.createOne(payload, callback);
    };
    this.readOne = function(payload, callback){
        var inst = new repository_core(_connector);
        inst.readOne(payload, callback);
    };
    this.updateOne = function(payload, callback){
        var inst = new repository_core(_connector);
        inst.updateOne(payload, callback);
    };
    this.deleteOne = function(payload, callback){
        var inst = new repository_core(_connector);
        inst.deleteOne(payload, callback);
    };

    this.createMany = function(payload, callback){
        var inst = new repository_core(_connector);
        inst.createMany(payload, callback);
    };
    this.readMany = function(payload, callback){
        var inst = new repository_core(_connector);
        inst.readMany(payload, callback);
    };
    this.updateMany = function(payload, callback){
        var inst = new repository_core(_connector);
        inst.updateMany(payload, callback);
    };
    this.deleteMany = function(payload, callback){
        var inst = new repository_core(_connector);
        inst.deleteMany(payload, callback);
    };

    this.aggregate = function(payload, callback){
        var inst = new repository_core(_connector);
        inst.aggregate(payload, callback);
    };
    this.count = function(payload, callback){
        var inst = new repository_core(_connector);
        inst.count(payload, callback);
    };
    this.createIndex = function(payload, callback){
        var inst = new repository_core(_connector);
        inst.createIndex(payload, callback);
    };

};

module.exports.new = function(url, options){
    count++;
    return new repository(url, options);

};