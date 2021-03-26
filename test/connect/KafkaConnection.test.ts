const assert = require('chai').assert;
const async = require('async');
const process = require('process');

import { ConfigParams } from 'pip-services3-commons-node';

import { KafkaConnection } from '../../src/connect/KafkaConnection';

suite('KafkaConnection', ()=> {
    let connection: KafkaConnection;

    let brokerHost = process.env['KAFKA_SERVICE_HOST'] || 'localhost';
    let brokerPort = process.env['KAFKA_SERVICE_PORT'] || 9092;
    if (brokerHost == '' && brokerPort == '') {
        return;
    }
    let brokerTopic = process.env['KAFKA_TOPIC'] || 'test';
    let brokerUser = process.env['KAFKA_USER']; // || 'kafka';
    let brokerPass = process.env['KAFKA_PASS']; // || 'pass123';

    setup(() => {
        let config = ConfigParams.fromTuples(
            'topic', brokerTopic,
            'connection.protocol', 'tcp',
            'connection.host', brokerHost,
            'connection.port', brokerPort,
            'credential.username', brokerUser,
            'credential.password', brokerPass,
            'credential.mechanism', 'plain'
        );        

        connection = new KafkaConnection();
        connection.configure(config);
    });

    test('Open/Close', (done) => {
        async.series([
            (callback) => {
                connection.open(null, (err) => {
                    assert.isNull(err);

                    assert.isTrue(connection.isOpen());
                    assert.isNotNull(connection.getConnection());

                    callback(err);
                });
            },
            (callback) => {
                connection.close(null, (err) => {
                    assert.isNull(err);

                    assert.isFalse(connection.isOpen());
                    assert.isNull(connection.getConnection());

                    callback(err);
                });
            }
        ], (err) => {
            done(err);
        });
    });

    test('ListTopics', (done) => {
        async.series([
            (callback) => {
                connection.open(null, (err) => {
                    assert.isNull(err);

                    assert.isTrue(connection.isOpen());
                    assert.isNotNull(connection.getConnection());

                    callback(err);
                });
            },
            (callback) => {
                connection.readTopicNames((err, topics) => {
                    assert.isNull(err);
                    assert.isArray(topics);

                    callback(err);
                });
            },
            (callback) => {
                connection.close(null, (err) => {
                    assert.isNull(err);

                    assert.isFalse(connection.isOpen());
                    assert.isNull(connection.getConnection());

                    callback(err);
                });
            }
        ], (err) => {
            done(err);
        });
    });    
});