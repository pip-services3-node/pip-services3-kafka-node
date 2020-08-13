let assert = require('chai').assert;
let async = require('async');
let process = require('process');

import { ConfigParams, IdGenerator } from 'pip-services3-commons-node';
import { References } from 'pip-services3-commons-node';
import { Descriptor } from 'pip-services3-commons-node';

import { MessageQueueFixture } from './MessageQueueFixture';
import { KafkaMessageQueue } from '../../src/queues/KafkaMessageQueue';
import { ConsoleLogger, LogLevel } from 'pip-services3-components-node';
import { logLevel } from 'kafkajs';

suite('KafkaMessageQueue', () => {
    let queue: KafkaMessageQueue;
    let fixture: MessageQueueFixture;
    let debug = true;

    var KAFKA_ENABLED = process.env['KAFKA_ENABLED'] || 'true';
    var KAFKA_URI = process.env['KAFKA_URI'];
    var KAFKA_HOST = process.env['KAFKA_HOST'] || 'localhost';
    var KAFKA_PORT = process.env['KAFKA_PORT'] || '9092';
    var KAFKA_TOPIC = process.env['KAFKA_TOPIC'] || 'test';
    var KAFKA_USER = process.env['KAFKA_USER'] || 'user';
    var KAFKA_PASS = process.env['KAFKA_PASS'] || 'pass123';

    if (KAFKA_ENABLED != 'true' && KAFKA_HOST == '' && KAFKA_PORT == '' && KAFKA_URI == '')
        return;

    let kafkaLogLevel = logLevel.NOTHING;
    let loggerLevel = LogLevel.None;

    if (debug)
    {
        kafkaLogLevel = logLevel.DEBUG;
        loggerLevel = LogLevel.Trace;
    }

    let queueConfig = ConfigParams.fromTuples(
        'log_level', kafkaLogLevel,
        'topic.name', KAFKA_TOPIC,
        'topic.num_partitions', 1,
        'topic.replication_factor', 1,

        'consumer.group_id', 'custom-group',

        'connection.uri', KAFKA_URI,
        'connection.host', KAFKA_HOST,
        'connection.port', KAFKA_PORT,
        'credential.username', KAFKA_USER,
        'credential.password', KAFKA_PASS,
        'credential.protocol', ''
    );

    setup((done) => {
        let correlationId = IdGenerator.nextShort();

        let logger = new ConsoleLogger();
        logger.setLevel(loggerLevel);

        let references: References = References.fromTuples(
            new Descriptor('pip-services', 'logger', 'console', 'default', '1.0'), logger,
        );

        queue = new KafkaMessageQueue(correlationId);
        queue.setReferences(references);

        queue.configure(queueConfig);

        fixture = new MessageQueueFixture(queue);

        queue.open(correlationId, (err: any) => {
            queue.clear(correlationId, (err) => {
                done(err);
            });
        });
    });

    teardown((done) => {
        queue.close(null, done);
    });
    
    test('Message Count', (done) => {
        fixture.testMessageCount(done);
    });  

    test('Send and Receive Message', (done) => {
        fixture.testSendReceiveMessage(done);
    });

    test('Receive and Send Message', (done) => {
        fixture.testReceiveSendMessage(done);
    });

    test('Receive and Complete Message', (done) => {
        fixture.testReceiveCompleteMessage(done);
    });  

    test('Receive and Abandon Message', (done) => {
        fixture.testReceiveAbandonMessage(done);
    });  

    test('Send and Peek Message', (done) => {
        fixture.testSendPeekMessage(done);
    });  

    test('Peek No Message', (done) => {
        fixture.testPeekNoMessage(done);
    });  

    test('On Message', (done) => {
        fixture.testOnMessage(done);
    });  

});