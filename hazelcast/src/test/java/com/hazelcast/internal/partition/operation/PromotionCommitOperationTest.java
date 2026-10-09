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

import static com.hazelcast.internal.partition.impl.MigrationInterceptor.MigrationParticipant.DESTINATION;
import static com.hazelcast.test.Accessors.getPartitionService;
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
    public void promotionRecovers_whenCompleteStageFails() {
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

    /**
     * Starts 2 members, installs the interceptor on the master and terminates the other member, so the master
     * promotes the backups it holds. Then asserts that the promotion completes and leaves no state behind.
     */
    private void assertPromotionRecovers(AtomicBoolean intercepted, MigrationInterceptor interceptor) {
        Config config = smallInstanceConfig()
                .setProperty(ClusterProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), String.valueOf(CALL_TIMEOUT_SECONDS));
        HazelcastInstance[] members = createHazelcastInstances(config, 2);
        HazelcastInstance master = members[0];
        warmUpPartitions(members);
        waitAllForSafeState(members);

        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(master);
        partitionService.setMigrationInterceptor(interceptor);

        members[1].getLifecycleService().terminate();
        assertClusterSizeEventually(1, master);

        assertTrueEventually(() -> assertTrue(intercepted.get()));
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

    private static void assertNoPartitionMigrating(InternalPartitionServiceImpl partitionService) {
        PartitionStateManager stateManager = partitionService.getPartitionStateManager();
        for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
            assertFalse("partition " + partitionId + " is still migrating", stateManager.isMigrating(partitionId));
        }
    }
}
