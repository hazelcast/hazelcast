package com.hazelcast.raft.service.countdownlatch.client;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.raft.service.countdownlatch.RaftCountDownLatchBasicTest;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;

public class RaftCountDownLatchClientBasicTest extends RaftCountDownLatchBasicTest {

    private HazelcastInstance client;

    @Override
    protected TestHazelcastInstanceFactory createTestFactory() {
        return new TestHazelcastFactory();
    }

    @Override
    protected HazelcastInstance[] createInstances() {
        HazelcastInstance[] instances = super.createInstances();
        TestHazelcastFactory f = (TestHazelcastFactory) factory;
        client = f.newHazelcastClient();
        return instances;
    }

    @Override
    protected ICountDownLatch createLatch(String name) {
        return RaftCountDownLatchProxy.create(client, name);
    }

    @After
    public void shutdown() {
        factory.terminateAll();
    }

}
