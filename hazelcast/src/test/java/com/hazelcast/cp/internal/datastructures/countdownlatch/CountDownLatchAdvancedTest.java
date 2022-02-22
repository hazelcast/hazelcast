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

package com.hazelcast.cp.internal.datastructures.countdownlatch;

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.internal.datastructures.spi.blocking.ResourceRegistry;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raftop.snapshot.RestoreSnapshotOp;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CountDownLatchAdvancedTest extends AbstractCountDownLatchAdvancedTest {

    private static final int LOG_ENTRY_COUNT_TO_SNAPSHOT = 10;

    private String objectName = "latch";
    private int groupSize = 3;

    @Override
    protected HazelcastInstance[] createInstances() {
        return newInstances(groupSize);
    }

    @Override
    protected String getName() {
        return objectName + "@group";
    }

    @Override
    protected ICountDownLatch createLatch(String name) {
        HazelcastInstance instance = instances[RandomPicker.getInt(instances.length)];
        return instance.getCPSubsystem().getCountDownLatch(name);
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(LOG_ENTRY_COUNT_TO_SNAPSHOT);
        return config;
    }

    @Test
    public void testNewRaftGroupMemberSchedulesTimeoutsWithSnapshot() throws ExecutionException, InterruptedException {
        latch.trySetCount(1);

        spawn(() -> {
            try {
                latch.await(10, MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        CPGroupId groupId = getGroupId(latch);

        assertTrueEventually(() -> {
            HazelcastInstance leader = getLeaderInstance(instances, groupId);
            CountDownLatchService service = getNodeEngineImpl(leader).getService(CountDownLatchService.SERVICE_NAME);
            ResourceRegistry registry = service.getRegistryOrNull(groupId);
            assertFalse(registry.getWaitTimeouts().isEmpty());
        });

        for (int i = 0; i < LOG_ENTRY_COUNT_TO_SNAPSHOT; i++) {
            latch.trySetCount(1);
        }

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                assertNotNull(raftNode);
                LogEntry snapshotEntry = getSnapshotEntry(raftNode);
                assertTrue(snapshotEntry.index() > 0);
                List<RestoreSnapshotOp> ops = (List<RestoreSnapshotOp>) snapshotEntry.operation();
                for (RestoreSnapshotOp op : ops) {
                    if (op.getServiceName().equals(CountDownLatchService.SERVICE_NAME)) {
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
        newInstance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                   .toCompletableFuture().get();

        assertTrueEventually(() -> {
            CountDownLatchService service = getNodeEngineImpl(newInstance).getService(CountDownLatchService.SERVICE_NAME);
            CountDownLatchRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertFalse(registry.getWaitTimeouts().isEmpty());
            Assert.assertEquals(1, registry.getRemainingCount(objectName));
        });
    }

    @Override
    protected HazelcastInstance leaderInstanceOf(CPGroupId groupId) {
        return getLeaderInstance(instances, groupId);
    }
}
