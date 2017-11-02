var mongo = require("./mongo");
var ObjectId = require('mongodb').ObjectId;
var Db = require('mongodb').Db;
var GridStore = require('mongodb').GridStore;


function addAsset(msg, callback){
    var res = {};
    if(msg.file && msg.file !== null) {
        mongo.getCollection('assets', function(err,coll){
            coll.count({
                original_name:msg.file.originalname,
                owner_id: new ObjectId(msg.user_id)
            }, function(err, count){
                if(err) {
                    res.code = 500;
                    res.message = "Internal server error";
                    callback(null, res);
                } else {
                    var new_filename = msg.file.originalname;
                    if(count > 0){
                        new_filename = [new_filename.substring(0, new_filename.lastIndexOf(".")), "(", count, ")",new_filename.substring(new_filename.lastIndexOf("."), new_filename.length)].join('');
                    }
                    var fileId = new ObjectId();
                    var gridStore = new GridStore(mongo.getDb(), fileId, new_filename, 'w', {root:'assets',content_type:msg.file.mimetype,chunk_size:msg.file.size});
                    gridStore.open(function(err, gridStore) {
                        gridStore.write(new Buffer(msg.buffer), function(err, gridResult) {
                            if (err) {
                                gridStore.close(function(err, gridResult) {
                                    res.code = 500;
                                    res.message = "Error saving file to database";
                                    callback(null, res);
                                });
                            } else {
                                gridStore.close(function(err, gridResult) {
                                    var curr_date = new Date();
                                    coll.insert({
                                        owner_id:new ObjectId(msg.user_id),
                                        name:new_filename,
                                        original_name:msg.file.originalname,
                                        file_id:fileId
                                    }, function(err,result){
                                        if(err) {
                                            res.code = 500;
                                            res.message = "Internal server error";
                                            callback(null, res);
                                        } else {
                                            res.code = 200;
                                            res.message = "Success";
                                            callback(null, res);
                                        }     
                                    });
                                });
                            }
                        })
                    })
                }
            })
        })
    } else {
        res.code = 400;
        res.message = "Fields missing";
        callback(null, res);
    }
}

function getAssets(msg, callback){
    console.log("getting assets in kafka...")
    var res = {};
    mongo.getCollection('assets', function(err,coll){
        coll.find({
            owner_id:new ObjectId(msg.user_id)
        },
        {
            name:true,
            _id:true
        }).toArray(function(err,result){
            if(err) {
                res.code = 500;
                res.message = "Internal server error";
                callback(null, res);
            } else {
                res.code = 200;
                res.message = "Success";
                res.data = result;
                callback(null, res);
            }
        })
    })
}

exports.addAsset = addAsset;
exports.getAssets = getAssets;