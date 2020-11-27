/** @module queues */
/** @hidden */
let async = require('async');

import { Kafka, Consumer, Producer, ProducerConfig, ConsumerConfig, AdminConfig, Admin, ITopicConfig, IHeaders, Message, KafkaMessage, logLevel, RecordMetadata } from 'kafkajs';

import { ConnectionParams } from 'pip-services3-components-node';
import { CredentialParams } from 'pip-services3-components-node';

import { IMessageReceiver } from 'pip-services3-messaging-node';
import { MessageQueue } from 'pip-services3-messaging-node';
import { MessageEnvelope } from 'pip-services3-messaging-node';
import { MessagingCapabilities } from 'pip-services3-messaging-node';

import { KafkaConnectionResolver } from '../connect/KafkaConnectionResolver';
import { ConfigParams } from 'pip-services3-commons-node';
import { isBuffer } from 'util';

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
let MSG_HEADER_TYPE = 'type'
let MSG_HEADER_CORRELATIONID = 'correlationId';

export class KafkaMessageQueue extends MessageQueue {

    private _client: Kafka;

    private _subscribed: boolean = false;
    private _optionsResolver: KafkaConnectionResolver = new KafkaConnectionResolver();
    private _receiver: IMessageReceiver;
    private _messages: MessageEnvelope[];

    private _adminClientConfig: AdminConfig;
    private _producerConfig: ProducerConfig;
    private _consumerConfig: ConsumerConfig;

    private _topicSpec: ITopicConfig;
    private _logLevel: number = logLevel.NOTHING;

    private _adminClient: Admin;
    private _consumer: Consumer;
    private _producer: Producer;

    /**
     * Creates a new instance of the message queue.
     * 
     * @param name  (optional) a queue name.
     */
    public constructor(name?: string) {
        super(name);

        this._capabilities = new MessagingCapabilities(false, true, true, false, false, false, true, false, false);
    }

    /**
	 * Checks if the component is opened.
	 * 
	 * @returns true if the component has been opened and false otherwise.
     */
    public isOpen(): boolean {
        return this._client != null && this._producer != null && this._adminClient != null;
    }

    /**
     * Configures component by passing configuration parameters.
     * 
     * @param config configuration parameters to be set
     */
    public configure(config: ConfigParams) {
        super.configure(config);

        this._logLevel = config.getAsIntegerWithDefault('log_level', this._logLevel);
        if (this._logLevel < logLevel.NOTHING) this._logLevel = logLevel.NOTHING;
        if (this._logLevel > logLevel.DEBUG) this._logLevel = logLevel.DEBUG;

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
    protected openWithParams(correlationId: string, connection: ConnectionParams, credential: CredentialParams, callback: (err: any) => void): void {
        this._logger.debug(correlationId, "openWithParams");

        this._optionsResolver.compose(correlationId, connection, credential, (err, options) => {
            if (err) {
                callback(err);
                return;
            }

            this._client = new Kafka({
                clientId: this.getName(),
                brokers: [options.uri],
                logLevel: this._logLevel
            });

            let run = async () => {
                this._adminClient = this._client.admin(this._adminClientConfig);
                await this._adminClient.connect();
                await this._adminClient.createTopics({
                    waitForLeaders: true,
                    topics: [this._topicSpec]
                });

                this._consumer = this._client.consumer(this._consumerConfig);
                await this._consumer.connect();
                await this._consumer.subscribe({
                    topic: this._topicSpec.topic,
                    fromBeginning: false
                });

                this._producer = this._client.producer(this._producerConfig);
                this._producer.connect();
            };

            run().catch(err => {
                if (callback) callback(err);
            }).then(() => {
                this._logger.debug(correlationId, "openWithParams complited");
                this.subscribe(correlationId);

                if (callback) callback(null);
            });
        });
    }

    /**
	 * Closes component and frees used resources.
	 * 
	 * @param correlationId 	(optional) transaction id to trace execution through call chain.
     * @param callback 			callback function that receives error or null no errors occured.
     */
    public close(correlationId: string, callback: (err: any) => void): void {
        if (this._client == null) {
            callback(null);
            return;
        }

        this._messages = [];
        this._subscribed = false;
        this._receiver = null;

        this._client = null;

        let promises = [];

        if (this._consumer) promises.push(this._consumer.disconnect());
        if (this._producer) promises.push(this._producer.disconnect());
        if (this._adminClient) promises.push(this._adminClient.disconnect());

        Promise.all(promises).catch(err => {
            if (callback) callback(err);
        }).then(() => {
            this._consumer = null;
            this._producer = null;
            this._adminClient = null;

            this._logger.trace(correlationId, 'Closed queue [%s]', this.getName());
            if (callback) callback(null);
        });
    }

    /**
	 * Clears component state.
	 * 
	 * @param correlationId 	(optional) transaction id to trace execution through call chain.
     * @param callback 			callback function that receives error or null no errors occured.
     */
    public clear(correlationId: string, callback: (err?: any) => void): void {
        this._messages = [];
        if (callback) callback();
    }

    /**
     * Reads the current number of messages in the queue to be delivered.
     * 
     * @param callback      callback function that receives number of messages or error.
     */
    public readMessageCount(callback: (err: any, count: number) => void): void {
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
    public send(correlationId: string, message: MessageEnvelope, callback?: (err: any) => void): void {
        this._counters.incrementOne('queue.' + this.getName() + '.sent_messages');
        this._logger.debug(message.correlation_id, 'Sent message %s via [%s]', message, this.getName());

        var envelope = this.toEnvelope(message);
        this._producer.send({
            topic: this._topicSpec.topic,
            messages: [envelope]
        }).catch(err => {
            if (callback) callback(err);
        }).then(result => {
            if (callback) callback(null);
        });
    }

    /**
     * Receives an incoming message and removes it from the queue.
     * 
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     * @param waitTimeout       a timeout in milliseconds to wait for a message to come.
     * @param callback          callback function that receives a message or error.
     */
    public receive(correlationId: string, waitTimeout: number, callback: (err: any, result: MessageEnvelope) => void): void {
        let err: any = null;
        let message: MessageEnvelope = null;

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
        async.whilst(
            () => {
                return this._client && i < waitTimeout && message == null;
            },
            (whilstCallback) => {
                i = i + checkIntervalMs;

                setTimeout(() => {
                    message = this._messages.shift();
                    whilstCallback();
                }, checkIntervalMs);
            },
            (err) => {
                callback(err, message);
            }
        );
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
    public abandon(message: MessageEnvelope, callback: (err: any) => void): void {
        // Make the message immediately visible
        var envelope = this.toEnvelope(this.toMessage(message.getReference() as KafkaMessage));
        if (envelope == null) {
            if (callback) callback(null);
            return;
        }

        this._producer.send({
            topic: this._topicSpec.topic,
            messages: [envelope]
        }).catch(err => {
            if (callback) callback(err);
        }).then(result => {
            message.setReference(null);
            this._logger.trace(message.correlation_id, 'Abandoned message %s at [%s]', message, this.getName());
            if (callback) callback(null);
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
    public listen(correlationId: string, receiver: IMessageReceiver): void {
        this._receiver = receiver;

        // Pass all cached messages
        async.whilst(
            () => {
                return this._messages.length > 0 && this._receiver != null;
            },
            (whilstCallback) => {
                if (this._messages.length > 0 && this._receiver != null) {
                    let message = this._messages.shift();
                    receiver.receiveMessage(message, this, whilstCallback);
                } else whilstCallback();
            },
            (err) => {
                // Subscribe to get messages
                this.subscribe(correlationId);
            }
        );
    }

    /**
     * Ends listening for incoming messages.
     * When this method is call [[listen]] unblocks the thread and execution continues.
     * 
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     */
    public endListen(correlationId: string): void {
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
    public peek(correlationId: string, callback: (err: any, result: MessageEnvelope) => void): void {
        // Not supported
        if (callback) callback(null, null);
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
    public peekBatch(correlationId: string, messageCount: number, callback: (err: any, result: MessageEnvelope[]) => void): void {
        // Not supported
        if (callback) callback(null, null);
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
    public renewLock(message: MessageEnvelope, lockTimeout: number, callback?: (err: any) => void): void {
        // Not supported
        if (callback) callback(null);
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
    public complete(message: MessageEnvelope, callback: (err: any) => void): void {
        // Operation is not supported
        message.setReference(null);
        if (callback) callback(null);
    }

    /**
     * Permanently removes a message from the queue and sends it to dead letter queue.
     * 
     * Important: This method is not supported by MQTT.
     * 
     * @param message   a message to be removed.
     * @param callback  (optional) callback function that receives an error or null for success.
     */
    public moveToDeadLetter(message: MessageEnvelope, callback: (err: any) => void): void {
        // Not supported
        if (callback) callback(null);
    }

    /**
     * Subscribes to the topic.
     */
    protected subscribe(correlationId: string): void {
        // Exit if already subscribed
        if (this._subscribed) {
            return;
        }

        this._subscribed = true;

        this._logger.trace(correlationId, "Started listening messages at [%s]", this.getName());

        // Subscribe to the topic
        this._consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                let envelop = this.toMessage(message);

                this._logger.debug(envelop.correlation_id, "Received message %s via [%s]", envelop, this.getName());

                let receiver = this._receiver;

                if (receiver != null) {
                    try {
                        receiver.receiveMessage(envelop, this, (err) => {
                            if (err) this._logger.error(correlationId, err, "Failed to receive the message");
                        });
                    } catch (ex) {
                        this._logger.error(correlationId, ex, "Failed to receive the message");
                    }
                } else {
                    // Keep message queue managable
                    while (this._messages.length > 1000)
                        this._messages.shift();

                    // Push into the message queue
                    this._messages.push(envelop);
                }
            }
        }).catch(err => {
            this._subscribed = false;
            if (err) this._logger.error(correlationId, err, "Failed to subscribe to topic " + this.getName());
        }).then(data => {
            this._logger.debug(correlationId, "Subscribed to the [%s]", this.getName());
        });
    }

    private toEnvelope(message: MessageEnvelope): Message {
        if (message == null) return null;

        let headers: IHeaders = {};
        headers[MSG_HEADER_TYPE] = Buffer.from(message.message_type);
        headers[MSG_HEADER_CORRELATIONID] = Buffer.from(message.correlation_id);

        let envelop: Message =
        {
            key: Buffer.from(message.message_id),
            value: Buffer.from(message.message),
            headers: headers,
            timestamp: message.sent_time?.toUTCString()
        };

        return envelop;
    }

    private toMessage(envelope: KafkaMessage): MessageEnvelope {
        if (envelope == null) return null;

        let messageType = this.getHeaderByKey(envelope.headers, MSG_HEADER_TYPE);
        let correlationId = this.getHeaderByKey(envelope.headers, MSG_HEADER_CORRELATIONID);

        let message = new MessageEnvelope(correlationId, messageType, envelope.value);
        message.message_id = envelope.key.toString();
        message.sent_time = new Date(+envelope.timestamp);
        message.setReference(envelope);

        return message;
    }

    private getHeaderByKey(headers: IHeaders, key: string): string {
        if (headers) {
            let value = headers[key];
            if (value) {
                return isBuffer(value) ? value.toString() : value.toString();
            }
        }

        return null;
    }
}