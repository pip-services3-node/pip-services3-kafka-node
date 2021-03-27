"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaConnectionResolver = void 0;
/** @module connect */
/** @hidden */
const async = require('async');
const pip_services3_commons_node_1 = require("pip-services3-commons-node");
const pip_services3_commons_node_2 = require("pip-services3-commons-node");
const pip_services3_components_node_1 = require("pip-services3-components-node");
const pip_services3_components_node_2 = require("pip-services3-components-node");
const pip_services3_components_node_3 = require("pip-services3-components-node");
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
class KafkaConnectionResolver {
    constructor() {
        /**
         * The connections resolver.
         */
        this._connectionResolver = new pip_services3_components_node_1.ConnectionResolver();
        /**
         * The credentials resolver.
         */
        this._credentialResolver = new pip_services3_components_node_2.CredentialResolver();
    }
    /**
     * Configures component by passing configuration parameters.
     *
     * @param config    configuration parameters to be set.
     */
    configure(config) {
        this._connectionResolver.configure(config);
        this._credentialResolver.configure(config);
    }
    /**
     * Sets references to dependent components.
     *
     * @param references 	references to locate the component dependencies.
     */
    setReferences(references) {
        this._connectionResolver.setReferences(references);
        this._credentialResolver.setReferences(references);
    }
    validateConnection(correlationId, connection) {
        if (connection == null) {
            return new pip_services3_commons_node_2.ConfigException(correlationId, "NO_CONNECTION", "Kafka connection is not set");
        }
        let uri = connection.getUri();
        if (uri != null)
            return null;
        let protocol = connection.getAsStringWithDefault("protocol", "tcp");
        if (protocol == null) {
            return new pip_services3_commons_node_2.ConfigException(correlationId, "NO_PROTOCOL", "Connection protocol is not set");
        }
        if (protocol != "tcp") {
            return new pip_services3_commons_node_2.ConfigException(correlationId, "UNSUPPORTED_PROTOCOL", "The protocol " + protocol + " is not supported");
        }
        let host = connection.getHost();
        if (host == null) {
            return new pip_services3_commons_node_2.ConfigException(correlationId, "NO_HOST", "Connection host is not set");
        }
        let port = connection.getAsIntegerWithDefault("port", 9092);
        if (port == 0) {
            return new pip_services3_commons_node_2.ConfigException(correlationId, "NO_PORT", "Connection port is not set");
        }
        return null;
    }
    parseUri(value, options) {
        if (value == null)
            return null;
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
    composeOptions(connections, credential) {
        // Define additional parameters parameters
        if (credential == null) {
            credential = new pip_services3_components_node_3.CredentialParams();
        }
        // Contruct options and copy over credentials
        let options = new pip_services3_commons_node_1.ConfigParams();
        options = options.setDefaults(credential);
        let globalUri = "";
        let brokers = "";
        // Process connections, find or constract uri
        for (let connection of connections) {
            if (globalUri != "") {
                continue;
            }
            let uri = connection.getUri();
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
        }
        else {
            options.setAsObject("brokers", brokers);
        }
        return options;
    }
    /**
     * Resolves Kafka connection options from connection and credential parameters.
     *
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     * @param callback 			callback function that receives resolved options or error.
     */
    resolve(correlationId, callback) {
        let connections;
        let credential;
        async.parallel([
            (callback) => {
                this._connectionResolver.resolveAll(correlationId, (err, result) => {
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
                this._credentialResolver.lookup(correlationId, (err, result) => {
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
    compose(correlationId, connections, credential, callback) {
        // Validate connections
        let err = null;
        for (let connection of connections) {
            err = err || this.validateConnection(correlationId, connection);
        }
        if (err)
            callback(err, null);
        else {
            let options = this.composeOptions(connections, credential);
            callback(null, options);
        }
    }
}
exports.KafkaConnectionResolver = KafkaConnectionResolver;
//# sourceMappingURL=KafkaConnectionResolver.js.map