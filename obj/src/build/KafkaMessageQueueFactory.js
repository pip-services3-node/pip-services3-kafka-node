"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaMessageQueueFactory = void 0;
/** @module build */
const pip_services3_commons_node_1 = require("pip-services3-commons-node");
const pip_services3_messaging_node_1 = require("pip-services3-messaging-node");
const KafkaMessageQueue_1 = require("../queues/KafkaMessageQueue");
/**
 * Creates [[KafkaMessageQueue]] components by their descriptors.
 * Name of created message queue is taken from its descriptor.
 *
 * @see [[https://pip-services3-node.github.io/pip-services3-components-node/classes/build.factory.html Factory]]
 * @see [[KafkaMessageQueue]]
 */
class KafkaMessageQueueFactory extends pip_services3_messaging_node_1.MessageQueueFactory {
    /**
     * Create a new instance of the factory.
     */
    constructor() {
        super();
        this.register(KafkaMessageQueueFactory.KafkaQueueDescriptor, (locator) => {
            let name = (typeof locator.getName === "function") ? locator.getName() : null;
            return this.createQueue(name);
        });
    }
    /**
     * Creates a message queue component and assigns its name.
     * @param name a name of the created message queue.
     */
    createQueue(name) {
        let queue = new KafkaMessageQueue_1.KafkaMessageQueue(name);
        if (this._config != null) {
            queue.configure(this._config);
        }
        if (this._references != null) {
            queue.setReferences(this._references);
        }
        return queue;
    }
}
exports.KafkaMessageQueueFactory = KafkaMessageQueueFactory;
KafkaMessageQueueFactory.KafkaQueueDescriptor = new pip_services3_commons_node_1.Descriptor("pip-services", "message-queue", "kafka", "*", "1.0");
//# sourceMappingURL=KafkaMessageQueueFactory.js.map