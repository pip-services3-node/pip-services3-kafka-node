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
exports.KafkaConnection = void 0;
/** @module connect */
const kafka = require('kafkajs');
const pip_services3_commons_node_1 = require("pip-services3-commons-node");
const pip_services3_commons_node_2 = require("pip-services3-commons-node");
const pip_services3_commons_node_3 = require("pip-services3-commons-node");
const pip_services3_components_node_1 = require("pip-services3-components-node");
const KafkaConnectionResolver_1 = require("./KafkaConnectionResolver");
/**
 * Kafka connection using plain driver.
 *
 * By defining a connection and sharing it through multiple message queues
 * you can reduce number of used database connections.
 *
 * ### Configuration parameters ###
 *
 * - connection(s):
 *   - discovery_key:             (optional) a key to retrieve the connection from [[https://pip-services3-node.github.io/pip-services3-components-node/interfaces/connect.idiscovery.html IDiscovery]]
 *   - host:                      host name or IP address
 *   - port:                      port number (default: 27017)
 *   - uri:                       resource URI or connection string with all parameters in it
 * - credential(s):
 *   - store_key:                 (optional) a key to retrieve the credentials from [[https://pip-services3-node.github.io/pip-services3-components-node/interfaces/auth.icredentialstore.html ICredentialStore]]
 *   - username:                  user name
 *   - password:                  user password
 * - options:
 *   - log_level:            (optional) log level 0 - None, 1 - Error, 2 - Warn, 3 - Info, 4 - Debug (default: 1)
 *   - connect_timeout:      (optional) number of milliseconds to connect to broker (default: 1000)
 *   - max_retries:          (optional) maximum retry attempts (default: 5)
 *   - retry_timeout:        (optional) number of milliseconds to wait on each reconnection attempt (default: 30000)
 *   - request_timeout:      (optional) number of milliseconds to wait on flushing messages (default: 30000)
 *
 * ### References ###
 *
 * - <code>\*:logger:\*:\*:1.0</code>           (optional) [[https://pip-services3-node.github.io/pip-services3-components-node/interfaces/log.ilogger.html ILogger]] components to pass log messages
 * - <code>\*:discovery:\*:\*:1.0</code>        (optional) [[https://pip-services3-node.github.io/pip-services3-components-node/interfaces/connect.idiscovery.html IDiscovery]] services
 * - <code>\*:credential-store:\*:\*:1.0</code> (optional) Credential stores to resolve credentials
 *
 */
class KafkaConnection {
    /**
     * Creates a new instance of the connection component.
     */
    constructor() {
        this._defaultConfig = pip_services3_commons_node_1.ConfigParams.fromTuples(
        // connections.*
        // credential.*
        "options.log_level", 1, "options.connect_timeout", 1000, "options.retry_timeout", 30000, "options.max_retries", 5, "options.request_timeout", 30000);
        /**
         * The logger.
         */
        this._logger = new pip_services3_components_node_1.CompositeLogger();
        /**
         * The connection resolver.
         */
        this._connectionResolver = new KafkaConnectionResolver_1.KafkaConnectionResolver();
        /**
         * The configuration options.
         */
        this._options = new pip_services3_commons_node_1.ConfigParams();
        /**
         * Topic subscriptions
         */
        this._subscriptions = [];
        this._logLevel = 1;
        this._connectTimeout = 1000;
        this._maxRetries = 5;
        this._retryTimeout = 30000;
        this._requestTimeout = 30000;
    }
    /**
     * Configures component by passing configuration parameters.
     *
     * @param config    configuration parameters to be set.
     */
    configure(config) {
        config = config.setDefaults(this._defaultConfig);
        this._connectionResolver.configure(config);
        this._options = this._options.override(config.getSection("options"));
        this._logLevel = config.getAsIntegerWithDefault("options.log_level", this._logLevel);
        this._connectTimeout = config.getAsIntegerWithDefault("options.connect_timeout", this._connectTimeout);
        this._maxRetries = config.getAsIntegerWithDefault("options.max_retries", this._maxRetries);
        this._retryTimeout = config.getAsIntegerWithDefault("options.retry_timeout", this._retryTimeout);
        this._requestTimeout = config.getAsIntegerWithDefault("options.request_timeout", this._requestTimeout);
    }
    /**
     * Sets references to dependent components.
     *
     * @param references 	references to locate the component dependencies.
     */
    setReferences(references) {
        this._logger.setReferences(references);
        this._connectionResolver.setReferences(references);
    }
    /**
     * Checks if the component is opened.
     *
     * @returns true if the component has been opened and false otherwise.
     */
    isOpen() {
        return this._connection != null;
    }
    /**
     * Opens the component.
     *
     * @param correlationId 	(optional) transaction id to trace execution through call chain.
     * @param callback 			callback function that receives error or null no errors occured.
     */
    open(correlationId, callback) {
        if (this._connection != null) {
            if (callback)
                callback(null);
            return;
        }
        this._connectionResolver.resolve(correlationId, (err, config) => {
            if (err) {
                if (callback)
                    callback(err);
                else
                    this._logger.error(correlationId, err, 'Failed to resolve NAS connection');
                return;
            }
            try {
                let options = {
                    retry: {
                        maxRetryType: this._requestTimeout,
                        retries: this._maxRetries
                    },
                    requestTimeout: this._requestTimeout,
                    connectionTimeout: this._connectTimeout,
                    logLevel: this._logLevel
                };
                let brokers = config.getAsString("brokers");
                options.brokers = brokers.split(",");
                options.ssl = config.getAsBoolean("ssl");
                let username = config.getAsString("username");
                let password = config.getAsString("password");
                let mechanism = config.getAsStringWithDefault("mechanism", "plain");
                if (username != null) {
                    options.sasl = {
                        mechanism: mechanism,
                        username: username,
                        password: password,
                    };
                }
                this._clientConfig = options;
                let connection = new kafka.Kafka(options);
                let producer = connection.producer();
                producer.connect()
                    .then(() => {
                    this._connection = connection;
                    this._producer = producer;
                    this._logger.debug(correlationId, "Connected to Kafka broker at " + brokers);
                    if (callback)
                        callback(null);
                })
                    .catch((err) => {
                    this._logger.error(correlationId, err, "Failed to connect to Kafka broker at " + brokers);
                    err = new pip_services3_commons_node_2.ConnectionException(correlationId, "CONNECT_FAILED", "Connection to Kafka service failed").withCause(err);
                    if (callback)
                        callback(err);
                });
            }
            catch (ex) {
                this._logger.error(correlationId, ex, "Failed to connect to Kafka server");
                let err = new pip_services3_commons_node_2.ConnectionException(correlationId, "CONNECT_FAILED", "Connection to Kafka service failed").withCause(ex);
                if (callback)
                    callback(err);
            }
        });
    }
    /**
     * Closes component and frees used resources.
     *
     * @param correlationId 	(optional) transaction id to trace execution through call chain.
     * @param callback 			callback function that receives error or null no errors occured.
     */
    close(correlationId, callback) {
        if (this._connection == null) {
            if (callback)
                callback(null);
            return;
        }
        // Disconnect producer
        this._producer.disconnect();
        this._producer = null;
        // Disconnect admin client
        if (this._adminClient != null) {
            this._adminClient.disconnect();
            this._adminClient = null;
        }
        // Disconnect consumers
        for (let subscription of this._subscriptions) {
            if (subscription.handler) {
                subscription.handler.disconnect();
            }
        }
        this._subscriptions = [];
        this._connection = null;
        this._logger.debug(correlationId, "Disconnected from Kafka server");
        if (callback)
            callback(null);
    }
    getConnection() {
        return this._connection;
    }
    getProducer() {
        return this._producer;
    }
    /**
     * Checks if connection is open
     * @returns an error is connection is closed or <code>null<code> otherwise.
     */
    checkOpen() {
        if (this.isOpen())
            return null;
        return new pip_services3_commons_node_3.InvalidStateException(null, "NOT_OPEN", "Connection was not opened");
    }
    /**
     * Connect admin client on demand.
     * @param callback a callback to get notification on connection result.
     */
    connectToAdmin(callback) {
        let err = this.checkOpen();
        if (err != null) {
            callback(err);
            return;
        }
        if (this._adminClient != null) {
            callback(null);
            return;
        }
        let adminClient = this._connection.admin();
        adminClient.connect()
            .then(() => {
            this._adminClient = adminClient;
            callback(null);
        })
            .catch((err) => {
            callback(err);
        });
    }
    /**
     * Reads a list of registered queue names.
     * If connection doesn't support this function returnes an empty list.
     * @callback to receive a list with registered queue names or an error.
     */
    readQueueNames(callback) {
        this.readTopicNames(callback);
    }
    /**
     * Reads a list of registered topic names.
     * If connection doesn't support this function returnes an empty list.
     * @callback to receive a list with registered topic names or an error.
     */
    readTopicNames(callback) {
        this.connectToAdmin((err) => {
            if (err != null) {
                callback(err, null);
                return;
            }
            this._adminClient.listTopics()
                .then((topicNames) => {
                callback(null, topicNames);
            })
                .catch((err) => {
                callback(err, null);
            });
        });
    }
    /**
     * Publish a message to a specified topic
     * @param topic a topic where the message will be placed
     * @param messages a list of messages to be published
     * @param options publishing options
     * @param callback (optional) callback to receive notification on operation result
     */
    publish(topic, messages, options, callback) {
        // Check for open connection
        let err = this.checkOpen();
        if (err) {
            if (callback)
                callback(err);
            return;
        }
        options = options || {};
        this._producer.send({
            topic: topic,
            messages: messages,
            acks: options.acks,
            timeout: options.timeout,
            compression: options.compression
        })
            .then(() => {
            if (callback)
                callback(null);
        })
            .catch((err) => {
            if (callback)
                callback(err);
        });
    }
    /**
     * Subscribe to a topic
     * @param subject a subject(topic) name
     * @param groupId (optional) a consumer group id
     * @param options subscription options
     * @param listener a message listener
     * @param callback (optional) callback to receive notification on operation result
     */
    subscribe(topic, groupId, options, listener, callback) {
        // Check for open connection
        let err = this.checkOpen();
        if (err != null) {
            if (callback)
                callback(err);
            return;
        }
        options = options || {};
        // Subscribe to topic
        let consumer = this._connection.consumer({
            groupId: groupId || "default",
            sessionTimeout: options.sessionTimeout,
            heartbeatInterval: options.heartbeatInterval,
            rebalanceTimeout: options.rebalanceTimeout,
            allowAutoTopicCreation: true
        });
        consumer.connect()
            .then(() => {
            consumer.subscribe({
                topic: topic,
                fromBeginning: options.fromBeginning,
            })
                .then(() => {
                consumer.run({
                    partitionsConsumedConcurrently: options.partitionsConsumedConcurrently,
                    autoCommit: options.autoCommit,
                    autoCommitInterval: options.autoCommitInterval,
                    autoCommitThreshold: options.autoCommitThreshold,
                    eachMessage: ({ topic, partition, message }) => __awaiter(this, void 0, void 0, function* () {
                        listener.onMessage(topic, partition, message);
                    })
                })
                    .then(() => {
                    // Add the subscription
                    let subscription = {
                        topic: topic,
                        groupId: groupId,
                        options: options,
                        handler: consumer,
                        listener: listener
                    };
                    this._subscriptions.push(subscription);
                    if (callback)
                        callback(null);
                })
                    .catch((err) => {
                    this._logger.error(null, err, "Failed to receive messages from Kafka consumer.");
                    consumer.disconnect();
                    if (callback)
                        callback(err);
                });
            })
                .catch((err) => {
                this._logger.error(null, err, "Failed to subscribe to Kafka consumer.");
                consumer.disconnect();
                if (callback)
                    callback(err);
            });
        })
            .catch((err) => {
            this._logger.error(null, err, "Failed to connect Kafka consumer.");
            if (callback)
                callback(err);
        });
    }
    /**
     * Unsubscribe from a previously subscribed topic
     * @param topic a topic name
     * @param groupId (optional) a consumer group id
     * @param listener a message listener
     * @param callback (optional) callback to receive notification on operation result
     */
    unsubscribe(topic, groupId, listener, callback) {
        // Find the subscription index
        let index = this._subscriptions.findIndex((s) => s.topic == topic && s.groupId == groupId && s.listener == listener);
        if (index < 0) {
            if (callback)
                callback(null);
            return;
        }
        // Remove the subscription
        let subscription = this._subscriptions.splice(index, 1)[0];
        // Unsubscribe from the topic
        if (this.isOpen() && subscription.handler != null) {
            subscription.handler.disconnect();
        }
        if (callback)
            callback(null);
    }
    /**
     * Commit a message offset.
     * @param topic a topic name
     * @param groupId (optional) a consumer group id
     * @param partition a partition number
     * @param offset a message offset
     * @param listener a message listener
     * @param callback (optional) callback to receive notification on operation result
     */
    commit(topic, groupId, partition, offset, listener, callback) {
        // Check for open connection
        let err = this.checkOpen();
        if (err != null) {
            if (callback)
                callback(err);
            return;
        }
        // Find the subscription index
        let subscription = this._subscriptions.find((s) => s.topic == topic && s.groupId == groupId && s.listener == listener);
        if (subscription == null || subscription.options.autoCommit) {
            if (callback)
                callback(null);
            return;
        }
        // Commit the offset
        subscription.handler.commitOffsets([
            { topic: topic, partition: partition, offset: offset }
        ])
            .then(() => {
            if (callback)
                callback(null);
        })
            .catch((err) => {
            if (callback)
                callback(err);
        });
    }
    /**
     * Seek a message offset.
     * @param topic a topic name
     * @param groupId (optional) a consumer group id
     * @param partition a partition number
     * @param offset a message offset
     * @param listener a message listener
     * @param callback (optional) callback to receive notification on operation result
     */
    seek(topic, groupId, partition, offset, listener, callback) {
        // Check for open connection
        let err = this.checkOpen();
        if (err != null) {
            if (callback)
                callback(err);
            return;
        }
        // Find the subscription index
        let subscription = this._subscriptions.find((s) => s.topic == topic && s.groupId == groupId && s.listener == listener);
        if (subscription == null || subscription.options.autoCommit) {
            if (callback)
                callback(null);
            return;
        }
        // Seek the offset
        subscription.handler.seek([
            { topic: topic, partition: partition, offset: offset }
        ])
            .then(() => {
            if (callback)
                callback(null);
        })
            .catch((err) => {
            if (callback)
                callback(err);
        });
    }
}
exports.KafkaConnection = KafkaConnection;
//# sourceMappingURL=KafkaConnection.js.map