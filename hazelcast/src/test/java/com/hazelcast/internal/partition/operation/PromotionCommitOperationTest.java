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
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PromotionCommitOperationTest extends HazelcastTestSupport {

    private static final int CALL_TIMEOUT_SECONDS = 5;

    @Test
    public void promotionCompletes_whenNextStageStartsAfterCallTimeout() {
        Config config = smallInstanceConfig()
                .setProperty(ClusterProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), String.valueOf(CALL_TIMEOUT_SECONDS));
        HazelcastInstance[] members = createHazelcastInstances(config, 2);
        HazelcastInstance master = members[0];
        warmUpPartitions(members);
        waitAllForSafeState(members);

        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(master);
        AtomicBoolean delayed = new AtomicBoolean();
        partitionService.setMigrationInterceptor(new MigrationInterceptor() {
            @Override
            public void onPromotionStart(MigrationParticipant participant, Collection<MigrationInfo> migrations) {
                // delays the BEFORE_PROMOTION stage, so the FINALIZE_PROMOTION stage starts after the call timeout
                if (participant == DESTINATION && delayed.compareAndSet(false, true)) {
                    sleepMillis((int) TimeUnit.SECONDS.toMillis(CALL_TIMEOUT_SECONDS + 2));
                }
            }
        });

        members[1].getLifecycleService().terminate();
        assertClusterSizeEventually(1, master);

        assertTrueEventually(() -> assertTrue(delayed.get()));
        assertTrueEventually(() -> assertEquals(0, partitionService.getMigrationQueueSize()));
        assertPromotionPermitReleased(partitionService.getMigrationManager());

        master.getCluster().changeClusterState(ClusterState.FROZEN);
        assertEquals(ClusterState.FROZEN, master.getCluster().getClusterState());
    }

    private static void assertPromotionPermitReleased(MigrationManager migrationManager) {
        assertTrue("promotion permit is still held", migrationManager.acquirePromotionPermit());
        migrationManager.releasePromotionPermit();
    }
}
