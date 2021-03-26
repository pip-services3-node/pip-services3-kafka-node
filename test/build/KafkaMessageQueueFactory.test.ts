const assert = require('chai').assert;

import { Descriptor } from 'pip-services3-commons-node';

import { KafkaMessageQueueFactory } from '../../src/build/KafkaMessageQueueFactory';

suite('KafkaMessageQueueFactory', ()=> {

    test('CreateMessageQueue', () => {
        let factory = new KafkaMessageQueueFactory();
        let descriptor = new Descriptor("pip-services", "message-queue", "kafka", "test", "1.0");
    
        let canResult = factory.canCreate(descriptor);
        assert.isNotNull(canResult);
    
        let queue = factory.create(descriptor);
        assert.isNotNull(queue);
        assert.equal("test", queue.getName());
    });

});