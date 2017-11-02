var kafka = require('./kafka/client');

var CHUNK_SIZE = 100 * 1024;

function sliceMyString(str){
    var slices = [];
    while(str !== ''){
        if(str.length > CHUNK_SIZE){
            slices.push(str.slice(0, CHUNK_SIZE));
            str = str.slice(CHUNK_SIZE);
        } else {
            slices.push(str);
            break;
        }
    }
    return slices;
}

function addAsset(req,res){
    var chunks = sliceMyString(req.file.buffer.toString('base64'));
    kafka.make_chunked_request('kafkademo','addAsset',{
        user_id:req.user._id,
        file:{
            originalname:req.file.originalname,
            mimetype:req.file.mimetype,
            size:req.file.size
        }
    }, chunks, function(err,result){
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