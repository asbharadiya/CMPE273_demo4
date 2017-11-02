var kafka = require('./kafka/client');
var crypto = require('crypto');

var CHUNK_SIZE = 100 * 1024;

function sliceMyString(str){
    var slices = [];
    while(str != ''){
        var lastSpace = 0;

        for(var i = 0; i < str.length && i < CHUNK_SIZE; i ++){
            if(str[i] == ' '){
                lastSpace = i;
            }
            if(i == str.length - 1){
                lastSpace = str.length;
            }
        }
        slices.push(str.slice(0, lastSpace));
        str = str.slice(lastSpace);
    }
    return slices;
}

function addAsset(req,res){
    // return res.status(200).json({status:200,statusText:"Internal server error"});
    var chunks = sliceMyString(req.file.buffer);
    kafka.make_chunked_request('kafkademo','addAsset',{
        user_id:req.user._id,
        file:{
            originalname:req.file.originalname,
            mimetype:req.file.mimetype,
            size:req.file.size
        }
    }, chunks, function(err,result){
        console.log(err);
        if(err) {
            return res.status(500).json({status:500,statusText:"Internal server error"});
        } else {
            return res.status(result.code).json({status:result.code,statusText:result.message});
        }
    })
}

function getAssets(req,res){
	kafka.make_request('kafkademo','getAssets',{
		user_id:req.user._id
	},function(err,result){
        if(err) {
           	return res.status(500).json({status:500,statusText:"Internal server error"});
        } else {
            return res.status(result.code).json({status:result.code,statusText:result.message,data:result.data});
        }
    });
}

exports.addAsset = addAsset;
exports.getAssets = getAssets;