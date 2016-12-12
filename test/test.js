/**
 * Created by vaio on 10.12.2016.
 */
var repository = require('nodejs.mongodb').repository;
var connector = require('nodejs.mongodb').connector;

var config = {
    host:'localhost',
    port:'27017',
    db:'',

    server:{}
};
repository.config();
repository.createOne(payload, function(err, res){});