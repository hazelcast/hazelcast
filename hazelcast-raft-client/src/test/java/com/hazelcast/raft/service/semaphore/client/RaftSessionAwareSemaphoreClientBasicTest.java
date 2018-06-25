package com.hazelcast.raft.service.semaphore.client;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.raft.service.exception.WaitKeyCancelledException;
import com.hazelcast.raft.service.semaphore.RaftSessionAwareSemaphoreBasicTest;
import com.hazelcast.raft.service.session.AbstractSessionManager;
import com.hazelcast.raft.service.session.SessionManagerProvider;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.raft.service.util.ClientAccessor.getClient;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftSessionAwareSemaphoreClientBasicTest extends RaftSessionAwareSemaphoreBasicTest {

    @Override
    protected TestHazelcastInstanceFactory createTestFactory() {
        return new TestHazelcastFactory();
    }

    @Override
    protected HazelcastInstance[] createInstances() {
        HazelcastInstance[] instances = super.createInstances();
        TestHazelcastFactory f = (TestHazelcastFactory) factory;
        semaphoreInstance = f.newHazelcastClient();
        SessionExpiredException.register(getClient(semaphoreInstance).getClientExceptionFactory());
        WaitKeyCancelledException.register(getClient(semaphoreInstance).getClientExceptionFactory());
        return instances;
    }

    @Override
    protected ISemaphore createSemaphore(String name) {
        return RaftSessionAwareSemaphoreProxy.create(semaphoreInstance, name);
    }

    @After
    public void shutdown() {
        factory.terminateAll();
    }

    @Override
    protected AbstractSessionManager getSessionManager(HazelcastInstance instance) {
        return SessionManagerProvider.get(getClient(instance));
    }

    @Override
    protected RaftGroupId getGroupId(ISemaphore semaphore) {
        return ((RaftSessionAwareSemaphoreProxy) semaphore).getGroupId();
    }

}
