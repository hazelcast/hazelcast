package com.hazelcast.queue;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.test.HazelcastTestSupport;

public abstract class AbstractQueueTest extends HazelcastTestSupport {

    protected IQueue newQueue() {
        HazelcastInstance instance = createHazelcastInstance();
        return instance.getQueue(randomString());
    }
}