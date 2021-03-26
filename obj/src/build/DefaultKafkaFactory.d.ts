/** @module build */
import { Factory } from 'pip-services3-components-node';
/**
 * Creates [[KafkaMessageQueue]] components by their descriptors.
 *
 * @see [[KafkaMessageQueue]]
 */
export declare class DefaultKafkaFactory extends Factory {
    private static readonly KafkaQueueDescriptor;
    private static readonly KafkaConnectionDescriptor;
    private static readonly KafkaQueueFactoryDescriptor;
    /**
     * Create a new instance of the factory.
     */
    constructor();
}
