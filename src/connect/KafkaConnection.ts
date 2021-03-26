/** @module connect */
/** @hidden */
const kafka = require('kafkajs');
/** @hidden */
const os = require('os');

import { IReferenceable } from 'pip-services3-commons-node';
import { IReferences } from 'pip-services3-commons-node';
import { IConfigurable } from 'pip-services3-commons-node';
import { IOpenable } from 'pip-services3-commons-node';
import { ConfigParams } from 'pip-services3-commons-node';
import { ConnectionException } from 'pip-services3-commons-node';
import { InvalidStateException } from 'pip-services3-commons-node';
import { CompositeLogger } from 'pip-services3-components-node';
import { IMessageQueueConnection } from 'pip-services3-messaging-node';

import { KafkaConnectionResolver } from './KafkaConnectionResolver';
import { IKafkaMessageListener } from './IKafkaMessageListener';
import { KafkaSubscription } from './KafkaSubscription';
import { openStdin } from 'node:process';
import { timeStamp } from 'node:console';

/**
 * Kafka connection using plain driver.
 * 
 * By defining a connection and sharing it through multiple message queues
 * you can reduce number of used database connections.
 * 
 * ### Configuration parameters ###
 * 
 * - client_id:               (optional) name of the client id
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
export class KafkaConnection implements IMessageQueueConnection, IReferenceable, IConfigurable, IOpenable {

    private _defaultConfig: ConfigParams = ConfigParams.fromTuples(
        // connections.*
        // credential.*

        "client_id", null,
        "options.log_level", 1,
        "options.connect_timeout", 1000,
        "options.retry_timeout", 30000,
        "options.max_retries", 5,
        "options.request_timeout", 30000
    );

    /** 
     * The logger.
     */
    protected _logger: CompositeLogger = new CompositeLogger();
    /**
     * The connection resolver.
     */
    protected _connectionResolver: KafkaConnectionResolver = new KafkaConnectionResolver();
    /**
     * The configuration options.
     */
    protected _options: ConfigParams = new ConfigParams();

    /**
     * The Kafka connection pool object.
     */
    protected _connection: any;
    /**
     * Kafka connection properties
     */
    protected _clientConfig: any;
    /**
     * The Kafka message producer object;
     */
    protected _producer: any;
    /**
     * The Kafka admin client object;
     */
    protected _adminClient: any;

    /**
     * Topic subscriptions
     */
    protected _subscriptions: KafkaSubscription[] = [];

    protected _clientId: string = os.hostname();
    protected _logLevel: number = 1;
    protected _connectTimeout: number = 1000;
    protected _maxRetries: number = 5;
    protected _retryTimeout: number = 30000;
    protected _requestTimeout: number = 30000;

    /**
     * Creates a new instance of the connection component.
     */
    public constructor() {}

    /**
     * Configures component by passing configuration parameters.
     * 
     * @param config    configuration parameters to be set.
     */
    public configure(config: ConfigParams): void {
        config = config.setDefaults(this._defaultConfig);
        this._connectionResolver.configure(config);
        this._options = this._options.override(config.getSection("options"));

        this._clientId = config.getAsStringWithDefault("client_id", this._clientId);
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
    public setReferences(references: IReferences): void {
        this._logger.setReferences(references);
        this._connectionResolver.setReferences(references);
    }

    /**
	 * Checks if the component is opened.
	 * 
	 * @returns true if the component has been opened and false otherwise.
     */
    public isOpen(): boolean {
        return this._connection != null;
    }

    /**
	 * Opens the component.
	 * 
	 * @param correlationId 	(optional) transaction id to trace execution through call chain.
     * @param callback 			callback function that receives error or null no errors occured.
     */
    public open(correlationId: string, callback?: (err: any) => void): void {
        if (this._connection != null) {
            if (callback) callback(null);
            return;
        }

        this._connectionResolver.resolve(correlationId, (err, config) => {
            if (err) {
                if (callback) callback(err);
                else this._logger.error(correlationId, err, 'Failed to resolve NAS connection');
                return;
            }

            try {                
                let options: any = {
                    clientId: this._clientId,
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
                    }
                }

                this._clientConfig = options;

                let connection = new kafka.Kafka(options);
                let producer = connection.producer();
                producer.connect()
                .then(() => {
                    this._connection = connection;
                    this._producer = producer;

                    this._logger.debug(correlationId, "Connected to Kafka broker at "+brokers);
                    if (callback) callback(null);
                })
                .catch((err) => {
                    this._logger.error(correlationId, err, "Failed to connect to Kafka broker at "+brokers);
                    err = new ConnectionException(correlationId, "CONNECT_FAILED", "Connection to Kafka service failed").withCause(err);
                    if (callback) callback(err);
                });
            } catch (ex) {
                this._logger.error(correlationId, ex, "Failed to connect to Kafka server");
                let err = new ConnectionException(correlationId, "CONNECT_FAILED", "Connection to Kafka service failed").withCause(ex);
                if (callback) callback(err);
            }
        });
    }

    /**
	 * Closes component and frees used resources.
	 * 
	 * @param correlationId 	(optional) transaction id to trace execution through call chain.
     * @param callback 			callback function that receives error or null no errors occured.
     */
    public close(correlationId: string, callback?: (err: any) => void): void {
        if (this._connection == null) {
            if (callback) callback(null);
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
        if (callback) callback(null);
    }

    public getConnection(): any {
        return this._connection;
    }

    public getProducer(): any {
        return this._producer;
    }

    /**
     * Checks if connection is open
     * @returns an error is connection is closed or <code>null<code> otherwise.
     */
     protected checkOpen(): any {
        if (this.isOpen()) return null;

        return new InvalidStateException(
            null,
            "NOT_OPEN",
            "Connection was not opened"
        );
    }    

    /**
     * Connect admin client on demand.
     * @param callback a callback to get notification on connection result.
     */
    protected connectToAdmin(callback: (err: any) => void): void {
        let err = this.checkOpen()
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
    public readQueueNames(callback: (err: any, queueNames: string[]) => void): void {
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
     * Creates a message queue.
     * If connection doesn't support this function it exists without error.
     * @param name the name of the queue to be created.
     * @param callback notifies about completion with error or null for success.
     */
    public createQueue(name: string, callback: (err: any) => void): void {
        // Todo: complete implementation
        if (callback) callback(null);
    }

    /**
     * Deletes a message queue.
     * If connection doesn't support this function it exists without error.
     * @param name the name of the queue to be deleted.
     * @param callback notifies about completion with error or null for success.
     */
    public deleteQueue(name: string, callback: (err: any) => void): void {
        // Todo: complete implementation
        if (callback) callback(null);
    }
     
    /**
     * Publish a message to a specified topic
     * @param topic a topic where the message will be placed
     * @param messages a list of messages to be published
     * @param options publishing options
     * @param callback (optional) callback to receive notification on operation result
     */
     public publish(topic: string, messages: any[], options: any, callback?: (err: any) => void): void {
        // Check for open connection
        let err = this.checkOpen();
        if (err) {
            if (callback) callback(err);
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
            if (callback) callback(null)
        })
        .catch((err) => {
            if (callback) callback(err);
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
     public subscribe(topic: string, groupId: string, options: any, listener: IKafkaMessageListener,
        callback?: (err: any) => void): void {
        // Check for open connection
        let err = this.checkOpen();
        if (err != null) {
            if (callback) callback(err);
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
            .then (() => {
                consumer.run({
                    partitionsConsumedConcurrently: options.partitionsConsumedConcurrently,
                    autoCommit: options.autoCommit,
                    autoCommitInterval: options.autoCommitInterval,
                    autoCommitThreshold: options.autoCommitThreshold,
                    eachMessage: async ({ topic, partition, message }) => {
                        listener.onMessage(topic, partition, message);
                    }
                })
                .then(() => {
                    // Add the subscription
                    let subscription = <KafkaSubscription>{
                        topic: topic,
                        groupId: groupId,
                        options: options,
                        handler: consumer,
                        listener: listener
                    };
                    this._subscriptions.push(subscription);

                    if (callback) callback(null);
                })
                .catch((err) => {
                    this._logger.error(null, err, "Failed to receive messages from Kafka consumer.");
                    consumer.disconnect();
                    if (callback) callback(err);
                });    
            })
            .catch((err) => {
                this._logger.error(null, err, "Failed to subscribe to Kafka consumer.");
                consumer.disconnect();
                if (callback) callback(err);
            });            
        })
        .catch((err) => {
            this._logger.error(null, err, "Failed to connect Kafka consumer.");
            if (callback) callback(err);
        });        
    }

    /**
     * Unsubscribe from a previously subscribed topic
     * @param topic a topic name
     * @param groupId (optional) a consumer group id
     * @param listener a message listener
     * @param callback (optional) callback to receive notification on operation result
     */
    public unsubscribe(topic: string, groupId: string, listener: IKafkaMessageListener, callback?: (err: any) => void): void {
        // Find the subscription index
        let index = this._subscriptions.findIndex((s) => s.topic == topic && s.groupId == groupId && s.listener == listener);
        if (index < 0) {
            if (callback) callback(null);
            return;
        }
        
        // Remove the subscription
        let subscription = this._subscriptions.splice(index, 1)[0];

        // Unsubscribe from the topic
        if (this.isOpen() && subscription.handler != null) {
            subscription.handler.disconnect();
        }

        if (callback) callback(null);
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
     public commit(topic: string, groupId: string, partition: number, offset: number, listener: IKafkaMessageListener,
        callback?: (err: any) => void): void {
        // Check for open connection
        let err = this.checkOpen();
        if (err != null) {
            if (callback) callback(err);
            return;
        }

        // Find the subscription index
        let subscription = this._subscriptions.find((s) => s.topic == topic && s.groupId == groupId && s.listener == listener);
        if (subscription == null || subscription.options.autoCommit) {
            if (callback) callback(null);
            return;
        }
        
        // Commit the offset
        subscription.handler.commitOffsets([
            { topic: topic, partition: partition, offset: offset }
        ])
        .then(() => {
            if (callback) callback(null);
        })
        .catch((err) => {
            if (callback) callback(err);
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
     public seek(topic: string, groupId: string, partition: number, offset: number, listener: IKafkaMessageListener,
        callback?: (err: any) => void): void {
        // Check for open connection
        let err = this.checkOpen();
        if (err != null) {
            if (callback) callback(err);
            return;
        }

        // Find the subscription index
        let subscription = this._subscriptions.find((s) => s.topic == topic && s.groupId == groupId && s.listener == listener);
        if (subscription == null || subscription.options.autoCommit) {
            if (callback) callback(null);
            return;
        }
        
        // Seek the offset
        subscription.handler.seek([
            { topic: topic, partition: partition, offset: offset }
        ])
        .then(() => {
            if (callback) callback(null);
        })
        .catch((err) => {
            if (callback) callback(err);
        });
    }          
}