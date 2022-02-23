/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cp.internal;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.raftop.metadata.TriggerDestroyRaftGroupOp;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RaftInvocationManagerTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;

    @Test
    public void when_raftGroupIsCreated_then_raftOperationsAreExecuted() throws ExecutionException, InterruptedException {
        int nodeCount = 5;
        instances = newInstances(nodeCount);

        RaftInvocationManager invocationService = getRaftInvocationManager(instances[0]);
        CPGroupId groupId = invocationService.createRaftGroup("test", nodeCount).get();

        for (int i = 0; i < 100; i++) {
            invocationService.invoke(groupId, new RaftTestApplyOp("val" + i)).get();
        }
    }

    @Test
    public void when_raftGroupIsCreated_then_raftOperationsAreExecutedOnNonCPNode() throws ExecutionException, InterruptedException {
        int cpNodeCount = 5;
        instances = newInstances(cpNodeCount, 3, 1);

        RaftInvocationManager invocationService = getRaftInvocationManager(instances[instances.length - 1]);
        CPGroupId groupId = invocationService.createRaftGroup("test", cpNodeCount).get();

        for (int i = 0; i < 100; i++) {
            invocationService.invoke(groupId, new RaftTestApplyOp("val" + i)).get();
        }
    }

    @Test
    public void when_raftGroupIsDestroyed_then_operationsEventuallyFail() throws ExecutionException, InterruptedException {
        int nodeCount = 3;
        instances = newInstances(nodeCount);

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
        CPGroupId groupId = invocationManager.createRaftGroup("test", nodeCount).get();

        invocationManager.invoke(groupId, new RaftTestApplyOp("val")).get();

        invocationManager.invoke(getRaftService(instances[0]).getMetadataGroupId(), new TriggerDestroyRaftGroupOp(groupId)).get();

        assertTrueEventually(() -> {
            try {
                invocationManager.invoke(groupId, new RaftTestApplyOp("val")).get();
                fail();
            } catch (ExecutionException e) {
                assertInstanceOf(CPGroupDestroyedException.class, e.getCause());
            }
        });
    }
}
