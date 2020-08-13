/** @module build */
import { Factory } from 'pip-services3-components-node';
import { Descriptor } from 'pip-services3-commons-node';

import { KafkaMessageQueue } from '../queues/KafkaMessageQueue';

/**
 * Creates [[KafkaMessageQueue]] components by their descriptors.
 * 
 * @see [[KafkaMessageQueue]]
 */
export class DefaultKafkaFactory extends Factory {
	public static readonly Descriptor = new Descriptor("pip-services", "factory", "kafka", "default", "1.0");
    public static readonly KafkaQueueDescriptor: Descriptor = new Descriptor("pip-services", "message-queue", "kafka", "*", "1.0");

	/**
	 * Create a new instance of the factory.
	 */
	public constructor() {
        super();
        this.register(DefaultKafkaFactory.KafkaQueueDescriptor, (locator: Descriptor) => {
            return new KafkaMessageQueue(locator.getName());
        });
	}
}