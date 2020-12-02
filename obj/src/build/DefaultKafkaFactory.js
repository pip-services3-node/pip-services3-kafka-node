"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DefaultKafkaFactory = void 0;
/** @module build */
const pip_services3_components_node_1 = require("pip-services3-components-node");
const pip_services3_commons_node_1 = require("pip-services3-commons-node");
const KafkaMessageQueue_1 = require("../queues/KafkaMessageQueue");
/**
 * Creates [[KafkaMessageQueue]] components by their descriptors.
 *
 * @see [[KafkaMessageQueue]]
 */
class DefaultKafkaFactory extends pip_services3_components_node_1.Factory {
    /**
     * Create a new instance of the factory.
     */
    constructor() {
        super();
        this.register(DefaultKafkaFactory.KafkaQueueDescriptor, (locator) => {
            return new KafkaMessageQueue_1.KafkaMessageQueue(locator.getName());
        });
    }
}
exports.DefaultKafkaFactory = DefaultKafkaFactory;
DefaultKafkaFactory.Descriptor = new pip_services3_commons_node_1.Descriptor("pip-services", "factory", "kafka", "default", "1.0");
DefaultKafkaFactory.KafkaQueueDescriptor = new pip_services3_commons_node_1.Descriptor("pip-services", "message-queue", "kafka", "*", "1.0");
//# sourceMappingURL=DefaultKafkaFactory.js.map