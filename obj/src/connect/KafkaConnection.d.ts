import { IReferenceable } from 'pip-services3-commons-node';
import { IReferences } from 'pip-services3-commons-node';
import { IConfigurable } from 'pip-services3-commons-node';
import { IOpenable } from 'pip-services3-commons-node';
import { ConfigParams } from 'pip-services3-commons-node';
import { CompositeLogger } from 'pip-services3-components-node';
import { IMessageQueueConnection } from 'pip-services3-messaging-node';
import { KafkaConnectionResolver } from './KafkaConnectionResolver';
import { IKafkaMessageListener } from './IKafkaMessageListener';
import { KafkaSubscription } from './KafkaSubscription';
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
export declare class KafkaConnection implements IMessageQueueConnection, IReferenceable, IConfigurable, IOpenable {
    private _defaultConfig;
    /**
     * The logger.
     */
    protected _logger: CompositeLogger;
    /**
     * The connection resolver.
     */
    protected _connectionResolver: KafkaConnectionResolver;
    /**
     * The configuration options.
     */
    protected _options: ConfigParams;
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
    protected _subscriptions: KafkaSubscription[];
    protected _logLevel: number;
    protected _connectTimeout: number;
    protected _maxRetries: number;
    protected _retryTimeout: number;
    protected _requestTimeout: number;
    /**
     * Creates a new instance of the connection component.
     */
    constructor();
    /**
     * Configures component by passing configuration parameters.
     *
     * @param config    configuration parameters to be set.
     */
    configure(config: ConfigParams): void;
    /**
     * Sets references to dependent components.
     *
     * @param references 	references to locate the component dependencies.
     */
    setReferences(references: IReferences): void;
    /**
     * Checks if the component is opened.
     *
     * @returns true if the component has been opened and false otherwise.
     */
    isOpen(): boolean;
    /**
     * Opens the component.
     *
     * @param correlationId 	(optional) transaction id to trace execution through call chain.
     * @param callback 			callback function that receives error or null no errors occured.
     */
    open(correlationId: string, callback?: (err: any) => void): void;
    /**
     * Closes component and frees used resources.
     *
     * @param correlationId 	(optional) transaction id to trace execution through call chain.
     * @param callback 			callback function that receives error or null no errors occured.
     */
    close(correlationId: string, callback?: (err: any) => void): void;
    getConnection(): any;
    getProducer(): any;
    /**
     * Checks if connection is open
     * @returns an error is connection is closed or <code>null<code> otherwise.
     */
    protected checkOpen(): any;
    /**
     * Connect admin client on demand.
     * @param callback a callback to get notification on connection result.
     */
    protected connectToAdmin(callback: (err: any) => void): void;
    /**
     * Reads a list of registered queue names.
     * If connection doesn't support this function returnes an empty list.
     * @callback to receive a list with registered queue names or an error.
     */
    readQueueNames(callback: (err: any, queueNames: string[]) => void): void;
    /**
     * Reads a list of registered topic names.
     * If connection doesn't support this function returnes an empty list.
     * @callback to receive a list with registered topic names or an error.
     */
    readTopicNames(callback: (err: any, topicNames: string[]) => void): void;
    /**
     * Publish a message to a specified topic
     * @param topic a topic where the message will be placed
     * @param messages a list of messages to be published
     * @param options publishing options
     * @param callback (optional) callback to receive notification on operation result
     */
    publish(topic: string, messages: any[], options: any, callback?: (err: any) => void): void;
    /**
     * Subscribe to a topic
     * @param subject a subject(topic) name
     * @param groupId (optional) a consumer group id
     * @param options subscription options
     * @param listener a message listener
     * @param callback (optional) callback to receive notification on operation result
     */
    subscribe(topic: string, groupId: string, options: any, listener: IKafkaMessageListener, callback?: (err: any) => void): void;
    /**
     * Unsubscribe from a previously subscribed topic
     * @param topic a topic name
     * @param groupId (optional) a consumer group id
     * @param listener a message listener
     * @param callback (optional) callback to receive notification on operation result
     */
    unsubscribe(topic: string, groupId: string, listener: IKafkaMessageListener, callback?: (err: any) => void): void;
    /**
     * Commit a message offset.
     * @param topic a topic name
     * @param groupId (optional) a consumer group id
     * @param partition a partition number
     * @param offset a message offset
     * @param listener a message listener
     * @param callback (optional) callback to receive notification on operation result
     */
    commit(topic: string, groupId: string, partition: number, offset: number, listener: IKafkaMessageListener, callback?: (err: any) => void): void;
    /**
     * Seek a message offset.
     * @param topic a topic name
     * @param groupId (optional) a consumer group id
     * @param partition a partition number
     * @param offset a message offset
     * @param listener a message listener
     * @param callback (optional) callback to receive notification on operation result
     */
    seek(topic: string, groupId: string, partition: number, offset: number, listener: IKafkaMessageListener, callback?: (err: any) => void): void;
}
