/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.MigrationInterceptor;
import com.hazelcast.internal.partition.impl.MigrationManager;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.spi.impl.operationparker.impl.OperationParkerImpl;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.partition.impl.MigrationInterceptor.MigrationParticipant.DESTINATION;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getOperationService;
import static com.hazelcast.test.Accessors.getPartitionService;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PromotionCommitOperationTest extends HazelcastTestSupport {

    private static final int CALL_TIMEOUT_SECONDS = 5;

    @Test
    public void promotionCompletes_whenNextStageStartsAfterCallTimeout() {
        AtomicBoolean intercepted = new AtomicBoolean();
        assertPromotionRecovers(intercepted, new MigrationInterceptor() {
            @Override
            public void onPromotionStart(MigrationParticipant participant, Collection<MigrationInfo> migrations) {
                // delays the BEFORE_PROMOTION stage, so the FINALIZE_PROMOTION stage starts after the call timeout
                if (participant == DESTINATION && intercepted.compareAndSet(false, true)) {
                    sleepMillis((int) TimeUnit.SECONDS.toMillis(CALL_TIMEOUT_SECONDS + 2));
                }
            }
        });
    }

    @Test
    public void promotionRecovers_whenBeforePromotionStageFails() {
        AtomicBoolean intercepted = new AtomicBoolean();
        assertPromotionRecovers(intercepted, new MigrationInterceptor() {
            @Override
            public void onPromotionStart(MigrationParticipant participant, Collection<MigrationInfo> migrations) {
                if (participant == DESTINATION && intercepted.compareAndSet(false, true)) {
                    throw new IllegalStateException("Injected failure in BEFORE_PROMOTION stage");
                }
            }
        });
    }

    @Test
    public void promotionCompletes_whenCompleteStageListenerFails() {
        AtomicBoolean intercepted = new AtomicBoolean();
        assertPromotionRecovers(intercepted, new MigrationInterceptor() {
            @Override
            public void onPromotionComplete(MigrationParticipant participant, Collection<MigrationInfo> migrations,
                                            boolean success) {
                if (participant == DESTINATION && intercepted.compareAndSet(false, true)) {
                    throw new IllegalStateException("Injected failure in COMPLETE stage");
                }
            }
        });
    }

    @Test
    public void promotionRollsBack_whenPartitionIsAlreadyMigrating() {
        HazelcastInstance[] members = startCluster();
        HazelcastInstance master = members[0];
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(master);
        PartitionStateManager stateManager = partitionService.getPartitionStateManager();

        // the master promotes the partitions of the other member, BeforePromotionOperation cannot set the migrating flag
        // of this partition while a migration or a replica sync keeps it
        int partitionId = getPartitionIdOwnedBy(partitionService, members[1]);
        assertTrue(stateManager.trySetMigratingFlag(partitionId));

        AtomicInteger failedPromotions = new AtomicInteger();
        partitionService.setMigrationInterceptor(new MigrationInterceptor() {
            @Override
            public void onPromotionComplete(MigrationParticipant participant, Collection<MigrationInfo> migrations,
                                            boolean success) {
                if (participant == DESTINATION && !success) {
                    failedPromotions.incrementAndGet();
                }
            }
        });

        members[1].getLifecycleService().terminate();
        assertClusterSizeEventually(1, master);

        assertTrueEventually(() -> assertTrue(failedPromotions.get() > 0));
        // the master waits before it retries the failed promotion, it does not retry in a busy loop
        int failedPromotionsBefore = failedPromotions.get();
        sleepSeconds(2);
        assertThat(failedPromotions.get() - failedPromotionsBefore).isBetween(1, 20);
        stateManager.clearMigratingFlag(partitionId);

        assertPromotionCompleted(master);
    }

    @Test
    public void promotionCompletes_whenFinalizePromotionOperationFails() {
        HazelcastInstance[] members = startCluster();
        HazelcastInstance master = members[0];
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(master);
        int partitionId = getPartitionIdOwnedBy(partitionService, members[1]);

        // FinalizePromotionOperation sends PartitionMigratingException to the operations parked on the promoted partition.
        // The response handler of this operation throws, so FinalizePromotionOperation fails.
        AtomicBoolean responseFailed = new AtomicBoolean();
        ParkedOperation parkedOperation = new ParkedOperation();
        parkedOperation.setPartitionId(partitionId).setReplicaIndex(1);
        parkedOperation.setOperationResponseHandler((op, response) -> {
            responseFailed.set(true);
            throw new IllegalStateException("Injected failure in FinalizePromotionOperation");
        });
        OperationParkerImpl operationParker = (OperationParkerImpl) getNodeEngineImpl(master).getOperationParker();
        getOperationService(master).execute(parkedOperation);
        assertTrueEventually(() -> assertEquals(1, operationParker.getTotalParkedOperationCount()));

        members[1].getLifecycleService().terminate();
        assertClusterSizeEventually(1, master);

        assertTrueEventually(() -> assertTrue(responseFailed.get()));
        assertPromotionCompleted(master);
    }

    /**
     * Starts 2 members, installs the interceptor on the master and terminates the other member, so the master
     * promotes the backups it holds. Then asserts that the promotion completes and leaves no state behind.
     */
    private void assertPromotionRecovers(AtomicBoolean intercepted, MigrationInterceptor interceptor) {
        HazelcastInstance[] members = startCluster();
        HazelcastInstance master = members[0];
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(master);
        partitionService.setMigrationInterceptor(interceptor);

        members[1].getLifecycleService().terminate();
        assertClusterSizeEventually(1, master);

        assertTrueEventually(() -> assertTrue(intercepted.get()));
        assertPromotionCompleted(master);
    }

    private HazelcastInstance[] startCluster() {
        Config config = smallInstanceConfig()
                .setProperty(ClusterProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), String.valueOf(CALL_TIMEOUT_SECONDS))
                .setProperty(ClusterProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "1");
        HazelcastInstance[] members = createHazelcastInstances(config, 2);
        warmUpPartitions(members);
        waitAllForSafeState(members);
        return members;
    }

    private static int getPartitionIdOwnedBy(InternalPartitionServiceImpl partitionService, HazelcastInstance member) {
        for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
            if (member.getCluster().getLocalMember().getAddress().equals(partitionService.getPartitionOwner(partitionId))) {
                return partitionId;
            }
        }
        throw new AssertionError("No partition is owned by " + member);
    }

    private static void assertPromotionCompleted(HazelcastInstance master) {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(master);
        assertTrueEventually(() -> assertEquals(0, partitionService.getMigrationQueueSize()));
        assertPromotionPermitReleased(partitionService.getMigrationManager());
        assertNoPartitionMigrating(partitionService);

        master.getCluster().changeClusterState(ClusterState.FROZEN);
        assertEquals(ClusterState.FROZEN, master.getCluster().getClusterState());
    }

    private static void assertPromotionPermitReleased(MigrationManager migrationManager) {
        assertTrue("promotion permit is still held", migrationManager.acquirePromotionPermit());
        migrationManager.releasePromotionPermit();
    }

    private static final class ParkedOperation extends Operation implements BlockingOperation {
        private static final WaitNotifyKey WAIT_KEY = new WaitNotifyKey() {
            @Override
            public String getServiceName() {
                return "promotion-test";
            }

            @Override
            public String getObjectName() {
                return "parked";
            }
        };

        @Override
        public WaitNotifyKey getWaitKey() {
            return WAIT_KEY;
        }

        @Override
        public boolean shouldWait() {
            return true;
        }

        @Override
        public void onWaitExpire() {
        }

        @Override
        public void run() {
        }
    }

    private static void assertNoPartitionMigrating(InternalPartitionServiceImpl partitionService) {
        PartitionStateManager stateManager = partitionService.getPartitionStateManager();
        for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
            assertFalse("partition " + partitionId + " is still migrating", stateManager.isMigrating(partitionId));
        }
    }
}
