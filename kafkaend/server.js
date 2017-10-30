var connection =  new require('./kafka/connection');

var auth = require('./services/auth');
var asset = require('./services/asset');

var mongo = require('./services/mongo');

var topic_name = 'kafkademo';


console.log('server is running');

var producer = connection.getProducer();    

connection.getConsumer(topic_name, function(consumer){
    consumer.on('message', function (message) {
        var data = JSON.parse(message.value);
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
    });
});
