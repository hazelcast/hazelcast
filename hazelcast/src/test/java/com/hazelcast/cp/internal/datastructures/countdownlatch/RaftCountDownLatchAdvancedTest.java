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

package com.hazelcast.cp.internal.datastructures.countdownlatch;

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.datastructures.countdownlatch.proxy.RaftCountDownLatchProxy;
import com.hazelcast.cp.internal.datastructures.spi.blocking.ResourceRegistry;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raftop.snapshot.RestoreSnapshotOp;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getSnapshotEntry;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftCountDownLatchAdvancedTest extends HazelcastRaftTestSupport {

    private static final int LOG_ENTRY_COUNT_TO_SNAPSHOT = 10;

    private HazelcastInstance[] instances;
    private ICountDownLatch latch;
    private String objectName = "latch";
    private String proxyName = objectName + "@group1";
    private int groupSize = 3;

    @Before
    public void setup() {
        instances = createInstances();
        latch = createLatch(proxyName);
        assertNotNull(latch);
    }

    private HazelcastInstance[] createInstances() {
        return newInstances(groupSize);
    }

    private ICountDownLatch createLatch(String name) {
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
    public void testSuccessfulAwaitClearsWaitTimeouts() {
        latch.trySetCount(1);

        CPGroupId groupId = getGroupId(latch);
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        final RaftCountDownLatchService service = getNodeEngineImpl(leader).getService(RaftCountDownLatchService.SERVICE_NAME);
        final RaftCountDownLatchRegistry registry = service.getRegistryOrNull(groupId);

        final CountDownLatch threadLatch = new CountDownLatch(1);
        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    latch.await(10, MINUTES);
                    threadLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(registry.getWaitTimeouts().isEmpty());
                assertFalse(registry.getLiveOperations().isEmpty());
            }
        });

        latch.countDown();

        assertOpenEventually(threadLatch);

        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }

    @Test
    public void testFailedAwaitClearsWaitTimeouts() throws InterruptedException {
        latch.trySetCount(1);

        CPGroupId groupId = getGroupId(latch);
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftCountDownLatchService service = getNodeEngineImpl(leader).getService(RaftCountDownLatchService.SERVICE_NAME);
        final RaftCountDownLatchRegistry registry = service.getRegistryOrNull(groupId);

        boolean success = latch.await(1, TimeUnit.SECONDS);

        assertFalse(success);
        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }

    @Test
    public void testDestroyClearsWaitTimeouts() {
        latch.trySetCount(1);

        CPGroupId groupId = getGroupId(latch);
        HazelcastInstance leader = getLeaderInstance(instances, groupId);
        RaftCountDownLatchService service = getNodeEngineImpl(leader).getService(RaftCountDownLatchService.SERVICE_NAME);
        final RaftCountDownLatchRegistry registry = service.getRegistryOrNull(groupId);

        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    latch.await(10, MINUTES);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(registry.getWaitTimeouts().isEmpty());
            }
        });

        latch.destroy();

        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }

    @Test
    public void testNewRaftGroupMemberSchedulesTimeoutsWithSnapshot() throws ExecutionException, InterruptedException {
        latch.trySetCount(1);

        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    latch.await(10, MINUTES);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        for (int i = 0; i < LOG_ENTRY_COUNT_TO_SNAPSHOT; i++) {
            latch.trySetCount(1);
        }

        final CPGroupId groupId = getGroupId(latch);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                HazelcastInstance leader = getLeaderInstance(instances, groupId);
                RaftCountDownLatchService service = getNodeEngineImpl(leader).getService(RaftCountDownLatchService.SERVICE_NAME);
                ResourceRegistry registry = service.getRegistryOrNull(groupId);
                assertFalse(registry.getWaitTimeouts().isEmpty());
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                    assertNotNull(raftNode);
                    LogEntry snapshotEntry = getSnapshotEntry(raftNode);
                    assertTrue(snapshotEntry.index() > 0);
                    List<RestoreSnapshotOp> ops = (List<RestoreSnapshotOp>) snapshotEntry.operation();
                    for (RestoreSnapshotOp op : ops) {
                        if (op.getServiceName().equals(RaftCountDownLatchService.SERVICE_NAME)) {
                            ResourceRegistry registry = (ResourceRegistry) op.getSnapshot();
                            assertFalse(registry.getWaitTimeouts().isEmpty());
                            return;
                        }
                    }
                    fail();
                }
            }
        });

        instances[1].shutdown();

        final HazelcastInstance newInstance = factory.newHazelcastInstance(createConfig(groupSize, groupSize));
        newInstance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember().get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftCountDownLatchService service = getNodeEngineImpl(newInstance).getService(RaftCountDownLatchService.SERVICE_NAME);
                RaftCountDownLatchRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertFalse(registry.getWaitTimeouts().isEmpty());
                Assert.assertEquals(1, registry.getRemainingCount(objectName));
            }
        });
    }

    private CPGroupId getGroupId(ICountDownLatch latch) {
        return ((RaftCountDownLatchProxy) latch).getGroupId();
    }
}
