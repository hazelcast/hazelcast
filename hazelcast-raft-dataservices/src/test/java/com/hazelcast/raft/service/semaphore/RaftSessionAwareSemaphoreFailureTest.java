package com.hazelcast.raft.service.semaphore;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.semaphore.proxy.RaftSessionAwareSemaphoreProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ThreadUtil;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.raft.service.session.SessionManagerService.NO_SESSION_ID;
import static com.hazelcast.raft.service.session.SessionManagerService.SERVICE_NAME;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftSessionAwareSemaphoreFailureTest extends RaftSemaphoreFailureTest {

    @Override
    boolean isStrictModeEnabled() {
        return true;
    }

    @Override
    RaftGroupId getGroupId(ISemaphore semaphore) {
        return ((RaftSessionAwareSemaphoreProxy) semaphore).getGroupId();
    }

    @Override
    long getSessionId(HazelcastInstance semaphoreInstance, RaftGroupId groupId) {
        SessionManagerService service = getNodeEngineImpl(semaphoreInstance).getService(SERVICE_NAME);
        long sessionId = service.getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        return sessionId;
    }

    @Override
    long getThreadId(HazelcastInstance semaphoreInstance, RaftGroupId groupId) {
        return ThreadUtil.getThreadId();
    }


}
