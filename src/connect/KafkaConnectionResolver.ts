/** @module connect */
/** @hidden */
const async = require('async');

import { IReferenceable } from 'pip-services3-commons-node';
import { IReferences } from 'pip-services3-commons-node';
import { IConfigurable } from 'pip-services3-commons-node';
import { ConfigParams } from 'pip-services3-commons-node';
import { ConfigException } from 'pip-services3-commons-node';
import { ConnectionResolver } from 'pip-services3-components-node';
import { CredentialResolver } from 'pip-services3-components-node';
import { ConnectionParams } from 'pip-services3-components-node';
import { CredentialParams } from 'pip-services3-components-node';

/**
 * Helper class that resolves Kafka connection and credential parameters,
 * validates them and generates connection options.
 * 
 *  ### Configuration parameters ###
 * 
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
 * - <code>\*:discovery:\*:\*:1.0</code>          (optional) [[https://pip-services3-node.github.io/pip-services3-components-node/interfaces/connect.idiscovery.html IDiscovery]] services to resolve connections
 * - <code>\*:credential-store:\*:\*:1.0</code>   (optional) Credential stores to resolve credentials
 */
export class KafkaConnectionResolver implements IReferenceable, IConfigurable {
    /** 
     * The connections resolver.
     */
    protected _connectionResolver: ConnectionResolver = new ConnectionResolver();
    /** 
     * The credentials resolver.
     */
    protected _credentialResolver: CredentialResolver = new CredentialResolver();

    /**
     * Configures component by passing configuration parameters.
     * 
     * @param config    configuration parameters to be set.
     */
    public configure(config: ConfigParams): void {
        this._connectionResolver.configure(config);
        this._credentialResolver.configure(config);
    }

    /**
	 * Sets references to dependent components.
	 * 
	 * @param references 	references to locate the component dependencies. 
     */
    public setReferences(references: IReferences): void {
        this._connectionResolver.setReferences(references);
        this._credentialResolver.setReferences(references);
    }

    private validateConnection(correlationId: string, connection: ConnectionParams): any {
        if (connection == null) {
            return new ConfigException(correlationId, "NO_CONNECTION", "Kafka connection is not set");
        }

        let uri = connection.getUri();
        if (uri != null) return null;

        let protocol = connection.getAsStringWithDefault("protocol", "tcp");
        if (protocol == null) {
            return new ConfigException(correlationId, "NO_PROTOCOL", "Connection protocol is not set");
        }
        if (protocol != "tcp") {
            return new ConfigException(correlationId, "UNSUPPORTED_PROTOCOL", "The protocol "+protocol+" is not supported");
        }

        let host = connection.getHost();
        if (host == null) {
            return new ConfigException(correlationId, "NO_HOST", "Connection host is not set");
        }

        let port = connection.getAsIntegerWithDefault("protocol", 9092);
        if (port == 0) {
            return new ConfigException(correlationId, "NO_PORT", "Connection port is not set");
        }

        return null;
    }

    private parseUri(value: string, options: ConfigParams): void {
        if (value == null) return null;

        let brokers = "";
        let uris = value.split(",");
        for (let uri of uris) {
            uri = uri.trim();
            
            let pos = uri.indexOf("?");
            uri = pos >= 0 ? uri.substring(0, pos) : uri;

            pos = uri.indexOf("://");
            uri = pos >= 0 ? uri.substring(pos + 3) : uri;

            pos = uri.indexOf("@");

            let server = pos > 0 ? uri.substring(pos + 1) : uri;
            if (brokers != "") {
                brokers += ",";
            }
            brokers += server;

            if (pos > 0) {
                let namePass = uri.substring(0, pos);
                pos = namePass.indexOf(":");
                let name = pos > 0 ? namePass.substring(0, pos) : namePass;
                let pass = pos > 0 ? namePass.substring(pos + 1) : "";
                options.setAsObject("username", name);
                options.setAsObject("password", pass);
            }
        }

        options.setAsObject("brokers", brokers);
    }

    private composeOptions(connections: ConnectionParams[], credential: CredentialParams): any {
        // Define additional parameters parameters
        if (credential == null) {
            credential = new CredentialParams();
        }

        // Contruct options and copy over credentials
        let options = new ConfigParams();
        options = options.setDefaults(credential);

        let globalUri = "";
        let brokers = "";

        // Process connections, find or constract uri
        for (let connection of connections) {
            if (globalUri != "") {
                continue;
            }

            let uri = connection.getUri()
            if (uri != null) {
                globalUri = uri;
                continue;
            }

            if (brokers != "") {
                brokers += ",";
            }

            let host = connection.getHost();
            brokers += host;

            let port = connection.getAsIntegerWithDefault("port", 9092);
            brokers += ":" + port;
        }

        // Set connection uri
        if (globalUri != "") {
            this.parseUri(globalUri, options);
        } else {
            options.setAsObject("brokers", brokers);
        }

        return options
    }

    /**
     * Resolves Kafka connection options from connection and credential parameters.
     * 
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     * @param callback 			callback function that receives resolved options or error.
     */
    public resolve(correlationId: string, callback: (err: any, options: any) => void): void {
        let connections: ConnectionParams[];
        let credential: CredentialParams;

        async.parallel([
            (callback) => {
                this._connectionResolver.resolveAll(correlationId, (err: any, result: ConnectionParams[]) => {
                    connections = result;

                    // Validate connections
                    if (err == null) {
                        for (let connection of connections) {
                            err = err || this.validateConnection(correlationId, connection);
                        }
                    }

                    callback(err);
                });
            },
            (callback) => {
                this._credentialResolver.lookup(correlationId, (err: any, result: CredentialParams) => {
                    credential = result;

                    // Credentials are not validated right now
                    callback(err);
                });
            }
        ], (err) => {
            if (err)
                callback(err, null);
            else {
                let options = this.composeOptions(connections, credential);
                callback(null, options);
            }
        });
    }

    /**
     * Composes Kafka connection options from connection and credential parameters.
     * 
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     * @param connections        connection parameters
     * @param credential        credential parameters
     * @param callback 			callback function that receives resolved options or error.
     */
    public compose(correlationId: string, connections: ConnectionParams[], credential: CredentialParams,
        callback: (err: any, options: any) => void): void {

        // Validate connections
        let err = null;
        for (let connection of connections) {
            err = err || this.validateConnection(correlationId, connection);
        }

        if (err) callback(err, null);
        else {
            let options = this.composeOptions(connections, credential);
            callback(null, options);
        }
    }
}
