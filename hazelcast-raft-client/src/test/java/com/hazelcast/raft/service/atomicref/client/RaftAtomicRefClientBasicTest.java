package com.hazelcast.raft.service.atomicref.client;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.atomicref.RaftAtomicRefBasicTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftAtomicRefClientBasicTest extends RaftAtomicRefBasicTest {

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
    protected <T> IAtomicReference<T> createAtomicRef(String name) {
        return RaftAtomicRefProxy.create(client, name);
    }

    @After
    public void shutdown() {
        factory.terminateAll();
    }

    @Override
    protected RaftGroupId getGroupId(IAtomicReference atomicRef) {
        return ((RaftAtomicRefProxy) atomicRef).getGroupId();
    }


}
