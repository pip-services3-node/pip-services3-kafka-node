let process = require('process');

import { ConfigParams } from 'pip-services3-commons-node';

import { MessageQueueFixture } from './MessageQueueFixture';
import { KafkaMessageQueue } from '../../src/queues/KafkaMessageQueue';

suite('KafkaMessageQueue', ()=> {
    let queue: KafkaMessageQueue;
    let fixture: MessageQueueFixture;

    let brokerHost = process.env['KAFKA_SERVICE_HOST'] || 'localhost';
    let brokerPort = process.env['KAFKA_SERVICE_PORT'] || 9092;
    if (brokerHost == '' && brokerPort == '') {
        return;
    }
    let brokerTopic = process.env['KAFKA_TOPIC'] || 'test';
    let brokerUser = process.env['KAFKA_USER']; // || 'kafka';
    let brokerPass = process.env['KAFKA_PASS']; // || 'pass123';

    let queueConfig = ConfigParams.fromTuples(
        'queue', brokerTopic,
        'connection.protocol', 'tcp',
        'connection.host', brokerHost,
        'connection.port', brokerPort,
        'credential.username', brokerUser,
        'credential.password', brokerPass,
        'credential.mechanism','plain',
    );        

    setup((done) => {
        queue = new KafkaMessageQueue(brokerTopic);
        queue.configure(queueConfig);

        fixture = new MessageQueueFixture(queue);

        queue.open(null, (err: any) => {
            // queue.clear(null, (err) => {
            //     done(err);
            // });

            done(err);
        });
    });

    teardown((done) => {
        queue.close(null, done);
    });

    test('Send and Receive Message', (done) => {
        fixture.testSendReceiveMessage(done);
    });
 
    test('Receive and Send Message', (done) => {
       fixture.testReceiveSendMessage(done);
    });

    test('Send Peek Message', (done) => {
        fixture.testSendPeekMessage(done);
    });

    test('Peek No Message', (done) => {
        fixture.testPeekNoMessage(done);
    });
      
    test('On Message', (done) => {
        fixture.testOnMessage(done);
    });

});