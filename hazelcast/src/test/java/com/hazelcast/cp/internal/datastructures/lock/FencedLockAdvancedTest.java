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

package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.spi.blocking.ResourceRegistry;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raftop.snapshot.RestoreSnapshotOp;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FencedLockAdvancedTest extends AbstractFencedLockAdvancedTest {

    private static final int LOG_ENTRY_COUNT_TO_SNAPSHOT = 10;

    private int groupSize = 3;

    @Override
    protected HazelcastInstance getPrimaryInstance() {
        return instances[RandomPicker.getInt(instances.length)];
    }

    @Override
    protected String getProxyName() {
        return objectName + "@group";
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(LOG_ENTRY_COUNT_TO_SNAPSHOT);
        cpSubsystemConfig.setSessionTimeToLiveSeconds(10);
        cpSubsystemConfig.setSessionHeartbeatIntervalSeconds(1);

        return config;
    }

    @Override
    protected HazelcastInstance[] createInstances() {
        return newInstances(groupSize);
    }

    @Test
    public void testNewRaftGroupMemberSchedulesTimeoutsWithSnapshot() throws ExecutionException, InterruptedException {
        long fence = this.lock.lockAndGetFence();
        assertTrue(fence > 0);

        spawn(() -> {
            lock.tryLock(10, MINUTES);
        });

        CPGroupId groupId = this.lock.getGroupId();

        assertTrueEventually(() -> {
            HazelcastInstance leader = getLeaderInstance(instances, groupId);
            LockService service = getNodeEngineImpl(leader).getService(LockService.SERVICE_NAME);
            ResourceRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertFalse(registry.getWaitTimeouts().isEmpty());
        });

        spawn(() -> {
            for (int i = 0; i < LOG_ENTRY_COUNT_TO_SNAPSHOT; i++) {
                lock.isLocked();
            }
        });

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                assertNotNull(raftNode);
                LogEntry snapshotEntry = getSnapshotEntry(raftNode);
                assertTrue(snapshotEntry.index() > 0);
                List<RestoreSnapshotOp> ops = (List<RestoreSnapshotOp>) snapshotEntry.operation();
                for (RestoreSnapshotOp op : ops) {
                    if (op.getServiceName().equals(LockService.SERVICE_NAME)) {
                        ResourceRegistry registry = (ResourceRegistry) op.getSnapshot();
                        assertFalse(registry.getWaitTimeouts().isEmpty());
                        return;
                    }
                }
                fail();
            }
        });

        HazelcastInstance instanceToShutdown = (instances[0] == proxyInstance) ? instances[1] : instances[0];
        instanceToShutdown.shutdown();

        HazelcastInstance newInstance = factory.newHazelcastInstance(createConfig(groupSize, groupSize));
        newInstance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                   .toCompletableFuture().get();

        assertTrueEventually(() -> {
            RaftNodeImpl raftNode = getRaftNode(newInstance, groupId);
            assertNotNull(raftNode);
            assertTrue(getSnapshotEntry(raftNode).index() > 0);

            LockService service = getNodeEngineImpl(newInstance).getService(LockService.SERVICE_NAME);
            LockRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertFalse(registry.getWaitTimeouts().isEmpty());
            LockOwnershipState ownership = registry.getLockOwnershipState(objectName);
            assertTrue(ownership.isLocked());
            assertTrue(ownership.getLockCount() > 0);
            assertEquals(fence, ownership.getFence());
        });
    }

    @Override
    protected HazelcastInstance leaderInstanceOf(CPGroupId groupId) {
        return getLeaderInstance(instances, groupId);
    }
}
