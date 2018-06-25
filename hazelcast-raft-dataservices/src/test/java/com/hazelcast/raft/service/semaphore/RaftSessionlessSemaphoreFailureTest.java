package com.hazelcast.raft.service.semaphore;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.atomiclong.operation.AddAndGetOp;
import com.hazelcast.raft.service.semaphore.proxy.RaftSessionlessSemaphoreProxy;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ConstructorFunction;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.raft.service.semaphore.proxy.GloballyUniqueThreadIdUtil.GLOBAL_THREAD_ID_GENERATOR_NAME;
import static com.hazelcast.raft.service.semaphore.proxy.GloballyUniqueThreadIdUtil.getGlobalThreadId;
import static com.hazelcast.raft.service.session.AbstractSessionManager.NO_SESSION_ID;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftSessionlessSemaphoreFailureTest extends RaftSemaphoreFailureTest {

    @Override
    RaftGroupId getGroupId(ISemaphore semaphore) {
        return ((RaftSessionlessSemaphoreProxy) semaphore).getGroupId();
    }

    @Override
    boolean isStrictModeEnabled() {
        return false;
    }

    @Override
    long getSessionId(HazelcastInstance semaphoreInstance, RaftGroupId groupId) {
        return NO_SESSION_ID;
    }

    @Override
    long getThreadId(final HazelcastInstance semaphoreInstance, RaftGroupId groupId) {
        return getGlobalThreadId(groupId, new ConstructorFunction<RaftGroupId, Long>() {
            @Override
            public Long createNew(RaftGroupId groupId) {
                InternalCompletableFuture<Long> f = getRaftInvocationManager(semaphoreInstance)
                        .invoke(groupId, new AddAndGetOp(GLOBAL_THREAD_ID_GENERATOR_NAME, 1));
                return f.join();
            }
        });
    }

}
