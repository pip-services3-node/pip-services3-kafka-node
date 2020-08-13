/** @module build */
import { Factory } from 'pip-services3-components-node';
import { Descriptor } from 'pip-services3-commons-node';
/**
 * Creates [[KafkaMessageQueue]] components by their descriptors.
 *
 * @see [[KafkaMessageQueue]]
 */
export declare class DefaultKafkaFactory extends Factory {
    static readonly Descriptor: Descriptor;
    static readonly KafkaQueueDescriptor: Descriptor;
    /**
     * Create a new instance of the factory.
     */
    constructor();
}
