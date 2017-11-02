var rpc = new (require('./rpc'))();

//make request to kafka
function make_request(queue_name, key, msg_payload, callback, chunkKey){
    rpc.makeRequest(queue_name, key, msg_payload, function(err, response){
		if(err) {
			callback(err);
		} else {
			callback(null, response);
		}
	},chunkKey)
}

exports.make_request = make_request;
