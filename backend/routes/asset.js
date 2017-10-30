var kafka = require('./kafka/client');

function addAsset(req,res){
    kafka.make_request('kafkademo','addAsset',{
		user_id:req.user._id,
        file:req.file
	},function(err,result){
        console.log(err);
        if(err) {
            return res.status(500).json({status:500,statusText:"Internal server error"});
        } else {
            return res.status(result.code).json({status:result.code,statusText:result.message});
        }
    });
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