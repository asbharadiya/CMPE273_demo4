var connection =  new require('./kafka/connection');

var auth = require('./services/auth');
var asset = require('./services/asset');

var mongo = require('./services/mongo');

var topic_name = 'kafkademo';

console.log('server is running');

var producer = connection.getProducer();   
var consumer = connection.getConsumer(topic_name); 

var chunk_requests = {};

consumer.on('message', function (message) {
    var data = JSON.parse(message.value)
    if(data.is_chunk_data){
        var chunk_request = chunk_requests[data.correlationId]
        if(chunk_request){
            chunk_request.chunks.push({
                data:data.chunk,
                order:data.chunk_no  
            })
        } else {
            chunk_request = {
                chunks:[
                    {
                        data:data.chunk,
                        order:data.chunk_no
                    }
                ],
                total_chunks:data.total_chunks
            }
            chunk_requests[data.correlationId] = chunk_request;
        }
        if(chunk_request.chunks.length === chunk_request.total_chunks){
            chunk_request.chunks.sort(function(a,b) {return (a.order > b.order) ? 1 : ((b.order > a.order) ? -1 : 0);});
            data.data.combined_chunks_data = "";
            for (var i=0;i<chunk_request.chunks.length;i++) {
                data.data.combined_chunks_data += chunk_request.chunks[i].data.data;
            }
            makeServiceCall(data);
        }
    } else {
        makeServiceCall(data);
    }
})

function makeServiceCall(data){
    switch (data.km.value) {
        case 'signin':
            auth.signin(data.data, function(err,res){
                var payloads = [
                    {   
                        topic: data.replyTo,
                        messages:JSON.stringify({
                            correlationId:data.correlationId,
                            data : res
                        }),
                        partition : 0
                    }
                ];
                producer.send(payloads, function(err, data){
                    //console.log(data);
                });
                return;
            });
            break;
        case 'signup':
            auth.signup(data.data, function(err,res){
                var payloads = [
                    {   
                        topic: data.replyTo,
                        messages:JSON.stringify({
                            correlationId:data.correlationId,
                            data : res
                        }),
                        partition : 0
                    }
                ];
                producer.send(payloads, function(err, data){
                    //console.log(data);
                });
                return;
            });
            break;
        case 'addAsset':
            asset.addAsset(data.data, function(err,res){
                var payloads = [
                    {   
                        topic: data.replyTo,
                        messages:JSON.stringify({
                            correlationId:data.correlationId,
                            data : res
                        }),
                        partition : 0
                    }
                ];
                producer.send(payloads, function(err, data){
                    //console.log(data);
                });
                return;
            });
            break;
        case 'getAssets':
            asset.getAssets(data.data, function(err,res){
                var payloads = [
                    {   
                        topic: data.replyTo,
                        messages:JSON.stringify({
                            correlationId:data.correlationId,
                            data : res
                        }),
                        partition : 0
                    }
                ];
                producer.send(payloads, function(err, data){
                    //console.log(data);
                });
                return;
            });
            break;
        default:
            return;
    }
}