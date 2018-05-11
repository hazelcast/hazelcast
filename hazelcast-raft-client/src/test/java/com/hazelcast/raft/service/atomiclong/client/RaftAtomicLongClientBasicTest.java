package com.hazelcast.raft.service.atomiclong.client;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongBasicTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftAtomicLongClientBasicTest extends RaftAtomicLongBasicTest {

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
    protected IAtomicLong createAtomicLong(String name) {
        return RaftAtomicLongProxy.create(client, name);
    }

    @After
    public void shutdown() {
        factory.terminateAll();
    }


    protected RaftGroupId getGroupId(IAtomicLong atomicLong) {
        return ((RaftAtomicLongProxy) atomicLong).getGroupId();
    }

    @Test
    @Ignore
    public void testAlter() {
    }

    @Test
    @Ignore
    public void testAlterAndGet() {
    }

    @Test
    @Ignore
    public void testGetAndAlter() {
    }

    @Test
    @Ignore
    public void testAlterAsync() {
    }

    @Test
    @Ignore
    public void testAlterAndGetAsync() {
    }

    @Test
    @Ignore
    public void testGetAndAlterAsync() {
    }

    @Test
    @Ignore
    public void testApply() {
    }

    @Test
    @Ignore
    public void testApplyAsync() {
    }

    @Test
    @Ignore
    public void testLocalGet_withLeaderLocalPolicy() {
    }

    @Test
    @Ignore
    public void testLocalGet_withAnyLocalPolicy() {
    }
}
