package com.hazelcast.raft.service.lock.client;

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.raft.service.lock.FencedLock;
import com.hazelcast.raft.service.lock.RaftFencedLockBasicTest;
import com.hazelcast.raft.service.exception.WaitKeyCancelledException;
import com.hazelcast.raft.service.session.AbstractSessionManager;
import com.hazelcast.raft.service.session.SessionManagerProvider;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.raft.service.util.ClientAccessor.getClient;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftFencedLockClientBasicTest extends RaftFencedLockBasicTest {

    @Override
    protected TestHazelcastInstanceFactory createTestFactory() {
        return new TestHazelcastFactory();
    }

    @Override
    protected HazelcastInstance[] createInstances() {
        HazelcastInstance[] instances = super.createInstances();
        TestHazelcastFactory f = (TestHazelcastFactory) factory;
        lockInstance = f.newHazelcastClient();
        HazelcastClientInstanceImpl client = getClient(lockInstance);
        SessionExpiredException.register(client.getClientExceptionFactory());
        WaitKeyCancelledException.register(client.getClientExceptionFactory());
        return instances;
    }

    @Override
    protected FencedLock createLock(String name) {
        return RaftFencedLockProxy.create(lockInstance, name);
    }

    protected AbstractSessionManager getSessionManager(HazelcastInstance instance) {
        return SessionManagerProvider.get(getClient(instance));
    }

}
