package com.hazelcast.raft.service.lock.client;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.raft.service.lock.RaftLockBasicTest;
import com.hazelcast.raft.service.exception.WaitKeyCancelledException;
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
public class RaftLockClientBasicTest extends RaftLockBasicTest {

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
        SessionExpiredException.register(getClient(client).getClientExceptionFactory());
        WaitKeyCancelledException.register(getClient(client).getClientExceptionFactory());
        return instances;
    }

    @Override
    protected ILock createLock(String name) {
        return RaftLockProxy.create(client, name);
    }

    @After
    public void shutdown() {
        factory.terminateAll();
    }

    @Override
    protected RaftGroupId getGroupId(ILock lock) {
        return ((RaftLockProxy) lock).getGroupId();
    }

    @Override
    protected AbstractSessionManager getSessionManager() {
        return SessionManagerProvider.get(getClient(client));
    }


}
