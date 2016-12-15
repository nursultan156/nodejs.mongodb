/**
 * Created by vaio on 10.12.2016.
 */
var repository = require('../dist/repository');
var async = require('async');

var r1 = repository.new('mongodb://localhost:27017/t1', {server: {socketOptions: {socketTimeoutMS: 1000 * 600}}});
var r2 = repository.new('mongodb://localhost:27017/t2', {server: {socketOptions: {socketTimeoutMS: 1000 * 600}}});

//console.log(r1);

var d1_1 = null;
var d1_2 = null;
var d2_1 = null;
var d2_2 = null;

async.series([
        function (callback) {
            d1_1 = new Date();
            var count = 0;
            async.whilst(
                function () {
                    return count < 1000000;
                },
                function (callback) {
                    count++;

                    var payload = {
                        collectionName: 'c1',
                        document: {
                            a: 1,
                            b: 2
                        }
                    };

                    r1.createOne(payload, function (err, res) {
                        if (err) return callback(err);
                        console.log(count);
                        callback(null);
                    });

                },
                function (err, n) {
                    d1_2 = new Date();
                    if (err) return callback(err);
                    callback(null);
                }
            );

        },
        function (callback) {
            d2_1 = new Date();
            var count = 0;
            async.whilst(
                function () {
                    return count < 1000000;
                },
                function (callback) {
                    count++;

                    var payload = {
                        collectionName: 'c2',
                        document: {
                            a: 1,
                            b: 2
                        }
                    };

                    r2.createOne(payload, function (err, res) {
                        if (err) return callback(err);
                        console.log(count);
                        callback(null);
                    });

                },
                function (err, n) {
                    d2_2 = new Date();
                    if (err) return callback(err);
                    callback(null);
                }
            );

        }
    ],
    function (err, res) {
        console.log(err);
        console.log(res);

        console.log(d1_2 - d1_1);
        console.log(d2_2 - d2_1);

    });

