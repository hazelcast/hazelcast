/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSemaphoreConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.datastructures.spi.blocking.ResourceRegistry;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raftop.snapshot.RestoreSnapshotOp;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getSnapshotEntry;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RaftSemaphoreAdvancedTest extends AbstractSemaphoreAdvancedTest {

    private static final int LOG_ENTRY_COUNT_TO_SNAPSHOT = 10;

    private int groupSize = 3;

    @Override
    protected HazelcastInstance[] createInstances() {
        return newInstances(groupSize);
    }

    @Override
    protected HazelcastInstance getPrimaryInstance() {
        return instances[RandomPicker.getInt(instances.length)];
    }

    @Override
    protected String getProxyName() {
        return objectName + "@group";
    }

    @Test
    public void testNewRaftGroupMemberSchedulesTimeoutsWithSnapshot() throws ExecutionException, InterruptedException {
        semaphore.init(1);

        spawn(() -> {
            try {
                semaphore.tryAcquire(2, 10, MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        for (int i = 0; i < LOG_ENTRY_COUNT_TO_SNAPSHOT; i++) {
            semaphore.acquire();
            semaphore.release();
        }

        CPGroupId groupId = getGroupId();

        assertTrueEventually(() -> {
            HazelcastInstance leader = leaderInstanceOf(groupId);
            RaftSemaphoreService service = getNodeEngineImpl(leader).getService(RaftSemaphoreService.SERVICE_NAME);
            RaftSemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertFalse(registry.getWaitTimeouts().isEmpty());
        });

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                assertNotNull(raftNode);
                LogEntry snapshotEntry = getSnapshotEntry(raftNode);
                assertTrue(snapshotEntry.index() > 0);
                List<RestoreSnapshotOp> ops = (List<RestoreSnapshotOp>) snapshotEntry.operation();
                for (RestoreSnapshotOp op : ops) {
                    if (op.getServiceName().equals(RaftSemaphoreService.SERVICE_NAME)) {
                        ResourceRegistry registry = (ResourceRegistry) op.getSnapshot();
                        assertFalse(registry.getWaitTimeouts().isEmpty());
                        return;
                    }
                }
                fail();
            }
        });

        instances[1].shutdown();

        HazelcastInstance newInstance = factory.newHazelcastInstance(createConfig(groupSize, groupSize));
        newInstance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember().get();

        assertTrueEventually(() -> {
            RaftSemaphoreService service = getNodeEngineImpl(newInstance).getService(RaftSemaphoreService.SERVICE_NAME);
            RaftSemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertFalse(registry.getWaitTimeouts().isEmpty());
            assertEquals(1, registry.availablePermits(objectName));
        });
    }

    @Override
    protected <T> InternalCompletableFuture<T> invokeRaftOp(RaftGroupId groupId, RaftOp op) {
        return getRaftInvocationManager(proxyInstance).invoke(groupId, op);
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(LOG_ENTRY_COUNT_TO_SNAPSHOT);
        cpSubsystemConfig.setSessionTimeToLiveSeconds(10);
        cpSubsystemConfig.setSessionHeartbeatIntervalSeconds(1);

        CPSemaphoreConfig semaphoreConfig = new CPSemaphoreConfig(objectName, false);
        cpSubsystemConfig.addSemaphoreConfig(semaphoreConfig);
        return config;
    }

    @Override
    protected HazelcastInstance leaderInstanceOf(CPGroupId groupId) {
        return getLeaderInstance(instances, groupId);
    }
}
