package com.hazelcast.raft.impl.service;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.exception.RaftGroupDestroyedException;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftInvocationManagerTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;

    @Test
    public void when_raftGroupIsCreated_then_raftOperationsAreExecuted() throws ExecutionException, InterruptedException {
        int nodeCount = 5;
        instances = newInstances(nodeCount);

        RaftInvocationManager invocationService = getRaftInvocationManager(instances[0]);
        final RaftGroupId groupId = invocationService.createRaftGroup("test", nodeCount).get();

        for (int i = 0; i < 100; i++) {
            invocationService.invoke(groupId, new RaftTestApplyOp("val" + i)).get();
        }
    }

    @Test
    public void when_raftGroupIsCreated_then_raftOperationsAreExecutedOnNonCPNode() throws ExecutionException, InterruptedException {
        int cpNodeCount = 5;
        instances = newInstances(cpNodeCount, 3, 1);

        RaftInvocationManager invocationService = getRaftInvocationManager(instances[instances.length - 1]);
        final RaftGroupId groupId = invocationService.createRaftGroup("test", cpNodeCount).get();

        for (int i = 0; i < 100; i++) {
            invocationService.invoke(groupId, new RaftTestApplyOp("val" + i)).get();
        }
    }

    @Test
    public void when_raftGroupIsDestroyed_then_operationsEventuallyFail() throws ExecutionException, InterruptedException {
        int nodeCount = 3;
        instances = newInstances(nodeCount);

        final RaftInvocationManager invocationService = getRaftInvocationManager(instances[0]);
        final RaftGroupId groupId = invocationService.createRaftGroup("test", nodeCount).get();

        invocationService.invoke(groupId, new RaftTestApplyOp("val")).get();

        invocationService.triggerDestroy(groupId).get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                try {
                    invocationService.invoke(groupId, new RaftTestApplyOp("val")).get();
                    fail();
                } catch (ExecutionException e) {
                    assertInstanceOf(RaftGroupDestroyedException.class, e.getCause());
                }
            }
        });
    }
}
