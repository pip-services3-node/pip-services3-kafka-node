"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaMessageQueue = void 0;
/** @module queues */
/** @hidden */
let async = require('async');
const kafkajs_1 = require("kafkajs");
const pip_services3_messaging_node_1 = require("pip-services3-messaging-node");
const pip_services3_messaging_node_2 = require("pip-services3-messaging-node");
const pip_services3_messaging_node_3 = require("pip-services3-messaging-node");
const KafkaConnectionResolver_1 = require("../connect/KafkaConnectionResolver");
const util_1 = require("util");
/**
 * Message queue that sends and receives messages via Kafka message broker.
 *
 * Kafka is a popular light-weight protocol to communicate IoT devices.
 *
 * ### Configuration parameters ###
 *
 * - topic:                         name of Kafka topic to subscribe
 * - connection(s):
 *   - discovery_key:               (optional) a key to retrieve the connection from [[https://pip-services3-node.github.io/pip-services3-components-node/interfaces/connect.idiscovery.html IDiscovery]]
 *   - host:                        host name or IP address
 *   - port:                        port number
 *   - uri:                         resource URI or connection string with all parameters in it
 * - credential(s):
 *   - store_key:                   (optional) a key to retrieve the credentials from [[https://pip-services3-node.github.io/pip-services3-components-node/interfaces/auth.icredentialstore.html ICredentialStore]]
 *   - username:                    user name
 *   - password:                    user password
 *
 * ### References ###
 *
 * - <code>\*:logger:\*:\*:1.0</code>             (optional) [[https://pip-services3-node.github.io/pip-services3-components-node/interfaces/log.ilogger.html ILogger]] components to pass log messages
 * - <code>\*:counters:\*:\*:1.0</code>           (optional) [[https://pip-services3-node.github.io/pip-services3-components-node/interfaces/count.icounters.html ICounters]] components to pass collected measurements
 * - <code>\*:discovery:\*:\*:1.0</code>          (optional) [[https://pip-services3-node.github.io/pip-services3-components-node/interfaces/connect.idiscovery.html IDiscovery]] services to resolve connections
 * - <code>\*:credential-store:\*:\*:1.0</code>   (optional) Credential stores to resolve credentials
 *
 * @see [[MessageQueue]]
 * @see [[MessagingCapabilities]]
 *
 * ### Example ###
 *
 *     let queue = new KafkaMessageQueue('myqueue');
 *     queue.configure(ConfigParams.fromTuples(
 *       'topic', 'mytopic',
 *       'connection.protocol', 'Kafka'
 *       'connection.host', 'localhost'
 *       'connection.port', 1883
 *     ));
 *
 *     queue.open('123', (err) => {
 *         ...
 *     });
 *
 *     queue.send('123', new MessageEnvelope(null, 'mymessage', 'ABC'));
 *
 *     queue.receive('123', (err, message) => {
 *         if (message != null) {
 *            ...
 *            queue.complete('123', message);
 *         }
 *     });
 */
let MSG_HEADER_TYPE = 'type';
let MSG_HEADER_CORRELATIONID = 'correlationId';
class KafkaMessageQueue extends pip_services3_messaging_node_1.MessageQueue {
    /**
     * Creates a new instance of the message queue.
     *
     * @param name  (optional) a queue name.
     */
    constructor(name) {
        super(name);
        this._subscribed = false;
        this._optionsResolver = new KafkaConnectionResolver_1.KafkaConnectionResolver();
        this._logLevel = kafkajs_1.logLevel.NOTHING;
        this._capabilities = new pip_services3_messaging_node_3.MessagingCapabilities(false, true, true, false, false, false, true, false, false);
    }
    /**
     * Checks if the component is opened.
     *
     * @returns true if the component has been opened and false otherwise.
     */
    isOpen() {
        return this._client != null && this._producer != null && this._adminClient != null;
    }
    /**
     * Configures component by passing configuration parameters.
     *
     * @param config configuration parameters to be set
     */
    configure(config) {
        super.configure(config);
        this._logLevel = config.getAsIntegerWithDefault('log_level', this._logLevel);
        if (this._logLevel < kafkajs_1.logLevel.NOTHING)
            this._logLevel = kafkajs_1.logLevel.NOTHING;
        if (this._logLevel > kafkajs_1.logLevel.DEBUG)
            this._logLevel = kafkajs_1.logLevel.DEBUG;
        this._topicSpec = {
            topic: config.getAsStringWithDefault('topic.name', this.getName()),
            numPartitions: config.getAsIntegerWithDefault('topic.num_partitions', 1),
            replicationFactor: config.getAsIntegerWithDefault('topic.replication_factor', 1),
        };
        this._consumerConfig = {
            groupId: config.getAsStringWithDefault('consumer.group_id', 'custom-group'),
        };
        this._producerConfig = {};
        this._adminClientConfig = {};
    }
    /**
     * Opens the component with given connection and credential parameters.
     *
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     * @param connection        connection parameters
     * @param credential        credential parameters
     * @param callback 			callback function that receives error or null no errors occured.
     */
    openWithParams(correlationId, connection, credential, callback) {
        this._logger.debug(correlationId, "openWithParams");
        this._optionsResolver.compose(correlationId, connection, credential, (err, options) => {
            if (err) {
                callback(err);
                return;
            }
            this._client = new kafkajs_1.Kafka({
                clientId: this.getName(),
                brokers: [options.uri],
                logLevel: this._logLevel
            });
            let run = () => __awaiter(this, void 0, void 0, function* () {
                this._adminClient = this._client.admin(this._adminClientConfig);
                yield this._adminClient.connect();
                yield this._adminClient.createTopics({
                    waitForLeaders: true,
                    topics: [this._topicSpec]
                });
                this._consumer = this._client.consumer(this._consumerConfig);
                yield this._consumer.connect();
                yield this._consumer.subscribe({
                    topic: this._topicSpec.topic,
                    fromBeginning: false
                });
                this._producer = this._client.producer(this._producerConfig);
                this._producer.connect();
            });
            run().catch(err => {
                if (callback)
                    callback(err);
            }).then(() => {
                this._logger.debug(correlationId, "openWithParams complited");
                this.subscribe(correlationId);
                if (callback)
                    callback(null);
            });
        });
    }
    /**
     * Closes component and frees used resources.
     *
     * @param correlationId 	(optional) transaction id to trace execution through call chain.
     * @param callback 			callback function that receives error or null no errors occured.
     */
    close(correlationId, callback) {
        if (this._client == null) {
            callback(null);
            return;
        }
        this._messages = [];
        this._subscribed = false;
        this._receiver = null;
        this._client = null;
        let promises = [];
        if (this._consumer)
            promises.push(this._consumer.disconnect());
        if (this._producer)
            promises.push(this._producer.disconnect());
        if (this._adminClient)
            promises.push(this._adminClient.disconnect());
        Promise.all(promises).catch(err => {
            if (callback)
                callback(err);
        }).then(() => {
            this._consumer = null;
            this._producer = null;
            this._adminClient = null;
            this._logger.trace(correlationId, 'Closed queue [%s]', this.getName());
            if (callback)
                callback(null);
        });
    }
    /**
     * Clears component state.
     *
     * @param correlationId 	(optional) transaction id to trace execution through call chain.
     * @param callback 			callback function that receives error or null no errors occured.
     */
    clear(correlationId, callback) {
        this._messages = [];
        if (callback)
            callback();
    }
    /**
     * Reads the current number of messages in the queue to be delivered.
     *
     * @param callback      callback function that receives number of messages or error.
     */
    readMessageCount(callback) {
        let count = this._messages.length;
        callback(null, count);
    }
    /**
     * Sends a message into the queue.
     *
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     * @param envelope          a message envelop to be sent.
     * @param callback          (optional) callback function that receives error or null for success.
     */
    send(correlationId, message, callback) {
        this._counters.incrementOne('queue.' + this.getName() + '.sent_messages');
        this._logger.debug(message.correlation_id, 'Sent message %s via [%s]', message, this.getName());
        var envelope = this.toEnvelope(message);
        this._producer.send({
            topic: this._topicSpec.topic,
            messages: [envelope]
        }).catch(err => {
            if (callback)
                callback(err);
        }).then(result => {
            if (callback)
                callback(null);
        });
    }
    /**
     * Receives an incoming message and removes it from the queue.
     *
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     * @param waitTimeout       a timeout in milliseconds to wait for a message to come.
     * @param callback          callback function that receives a message or error.
     */
    receive(correlationId, waitTimeout, callback) {
        let err = null;
        let message = null;
        // Subscribe to get messages
        this.subscribe(correlationId);
        // Return message immediately if it exist
        if (this._messages.length > 0) {
            message = this._messages.shift();
            callback(null, message);
            return;
        }
        // Otherwise wait and return
        let checkIntervalMs = 100;
        let i = 0;
        async.whilst(() => {
            return this._client && i < waitTimeout && message == null;
        }, (whilstCallback) => {
            i = i + checkIntervalMs;
            setTimeout(() => {
                message = this._messages.shift();
                whilstCallback();
            }, checkIntervalMs);
        }, (err) => {
            callback(err, message);
        });
    }
    /**
     * Returnes message into the queue and makes it available for all subscribers to receive it again.
     * This method is usually used to return a message which could not be processed at the moment
     * to repeat the attempt. Messages that cause unrecoverable errors shall be removed permanently
     * or/and send to dead letter queue.
     *
     * Important: This method is not supported by MQTT.
     *
     * @param message   a message to return.
     * @param callback  (optional) callback function that receives an error or null for success.
     */
    abandon(message, callback) {
        // Make the message immediately visible
        var envelope = this.toEnvelope(this.toMessage(message.getReference()));
        if (envelope == null) {
            if (callback)
                callback(null);
            return;
        }
        this._producer.send({
            topic: this._topicSpec.topic,
            messages: [envelope]
        }).catch(err => {
            if (callback)
                callback(err);
        }).then(result => {
            message.setReference(null);
            this._logger.trace(message.correlation_id, 'Abandoned message %s at [%s]', message, this.getName());
            if (callback)
                callback(null);
        });
    }
    /**
     * Listens for incoming messages and blocks the current thread until queue is closed.
     *
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     * @param receiver          a receiver to receive incoming messages.
     *
     * @see [[IMessageReceiver]]
     * @see [[receive]]
     */
    listen(correlationId, receiver) {
        this._receiver = receiver;
        // Pass all cached messages
        async.whilst(() => {
            return this._messages.length > 0 && this._receiver != null;
        }, (whilstCallback) => {
            if (this._messages.length > 0 && this._receiver != null) {
                let message = this._messages.shift();
                receiver.receiveMessage(message, this, whilstCallback);
            }
            else
                whilstCallback();
        }, (err) => {
            // Subscribe to get messages
            this.subscribe(correlationId);
        });
    }
    /**
     * Ends listening for incoming messages.
     * When this method is call [[listen]] unblocks the thread and execution continues.
     *
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     */
    endListen(correlationId) {
        this._receiver = null;
        if (this._subscribed) {
            this._subscribed = false;
        }
    }
    /**
     * Peeks a single incoming message from the queue without removing it.
     * If there are no messages available in the queue it returns null.
     *
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     * @param callback          callback function that receives a message or error.
     */
    peek(correlationId, callback) {
        // Not supported
        if (callback)
            callback(null, null);
    }
    /**
     * Peeks multiple incoming messages from the queue without removing them.
     * If there are no messages available in the queue it returns an empty list.
     *
     * Important: This method is not supported by MQTT.
     *
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     * @param messageCount      a maximum number of messages to peek.
     * @param callback          callback function that receives a list with messages or error.
     */
    peekBatch(correlationId, messageCount, callback) {
        // Not supported
        if (callback)
            callback(null, null);
    }
    /**
     * Renews a lock on a message that makes it invisible from other receivers in the queue.
     * This method is usually used to extend the message processing time.
     *
     * Important: This method is not supported by MQTT.
     *
     * @param message       a message to extend its lock.
     * @param lockTimeout   a locking timeout in milliseconds.
     * @param callback      (optional) callback function that receives an error or null for success.
     */
    renewLock(message, lockTimeout, callback) {
        // Not supported
        if (callback)
            callback(null);
    }
    /**
     * Permanently removes a message from the queue.
     * This method is usually used to remove the message after successful processing.
     *
     * Important: This method is not supported by MQTT.
     *
     * @param message   a message to remove.
     * @param callback  (optional) callback function that receives an error or null for success.
     */
    complete(message, callback) {
        // Operation is not supported
        message.setReference(null);
        if (callback)
            callback(null);
    }
    /**
     * Permanently removes a message from the queue and sends it to dead letter queue.
     *
     * Important: This method is not supported by MQTT.
     *
     * @param message   a message to be removed.
     * @param callback  (optional) callback function that receives an error or null for success.
     */
    moveToDeadLetter(message, callback) {
        // Not supported
        if (callback)
            callback(null);
    }
    /**
     * Subscribes to the topic.
     */
    subscribe(correlationId) {
        // Exit if already subscribed
        if (this._subscribed) {
            return;
        }
        this._subscribed = true;
        this._logger.trace(correlationId, "Started listening messages at [%s]", this.getName());
        // Subscribe to the topic
        this._consumer.run({
            eachMessage: ({ topic, partition, message }) => __awaiter(this, void 0, void 0, function* () {
                let envelop = this.toMessage(message);
                this._logger.debug(envelop.correlation_id, "Received message %s via [%s]", envelop, this.getName());
                let receiver = this._receiver;
                if (receiver != null) {
                    try {
                        receiver.receiveMessage(envelop, this, (err) => {
                            if (err)
                                this._logger.error(correlationId, err, "Failed to receive the message");
                        });
                    }
                    catch (ex) {
                        this._logger.error(correlationId, ex, "Failed to receive the message");
                    }
                }
                else {
                    // Keep message queue managable
                    while (this._messages.length > 1000)
                        this._messages.shift();
                    // Push into the message queue
                    this._messages.push(envelop);
                }
            })
        }).catch(err => {
            this._subscribed = false;
            if (err)
                this._logger.error(correlationId, err, "Failed to subscribe to topic " + this.getName());
        }).then(data => {
            this._logger.debug(correlationId, "Subscribed to the [%s]", this.getName());
        });
    }
    toEnvelope(message) {
        var _a;
        if (message == null)
            return null;
        let headers = {};
        headers[MSG_HEADER_TYPE] = Buffer.from(message.message_type);
        headers[MSG_HEADER_CORRELATIONID] = Buffer.from(message.correlation_id);
        let envelop = {
            key: Buffer.from(message.message_id),
            value: Buffer.from(message.message),
            headers: headers,
            timestamp: (_a = message.sent_time) === null || _a === void 0 ? void 0 : _a.toUTCString()
        };
        return envelop;
    }
    toMessage(envelope) {
        if (envelope == null)
            return null;
        let messageType = this.getHeaderByKey(envelope.headers, MSG_HEADER_TYPE);
        let correlationId = this.getHeaderByKey(envelope.headers, MSG_HEADER_CORRELATIONID);
        let message = new pip_services3_messaging_node_2.MessageEnvelope(correlationId, messageType, envelope.value);
        message.message_id = envelope.key.toString();
        message.sent_time = new Date(+envelope.timestamp);
        message.setReference(envelope);
        return message;
    }
    getHeaderByKey(headers, key) {
        if (headers) {
            let value = headers[key];
            if (value) {
                return util_1.isBuffer(value) ? value.toString() : value.toString();
            }
        }
        return null;
    }
}
exports.KafkaMessageQueue = KafkaMessageQueue;
//# sourceMappingURL=KafkaMessageQueue.js.map