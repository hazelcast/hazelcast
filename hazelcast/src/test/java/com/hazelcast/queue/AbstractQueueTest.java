package com.hazelcast.queue;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.test.HazelcastTestSupport;

public abstract class AbstractQueueTest extends HazelcastTestSupport {

    protected IQueue newQueue() {
        HazelcastInstance instance = createHazelcastInstance();
        return instance.getQueue(randomString());
    }

    protected IQueue newQueue_WithMaxSizeConfig(int maxSize) {
        Config config = new Config();
        final String name = randomString();
        config.getQueueConfig(name).setMaxSize(maxSize);
        HazelcastInstance instance = createHazelcastInstance(config);
        return instance.getQueue(name);
    }
}