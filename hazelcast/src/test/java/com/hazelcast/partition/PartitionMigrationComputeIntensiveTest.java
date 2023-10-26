/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.partition;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.partition.PartitionMigrationListenerTest.EventCollectingMigrationListener;
import static com.hazelcast.partition.PartitionMigrationListenerTest.MigrationEventsPack;
import static com.hazelcast.partition.PartitionMigrationListenerTest.assertMigrationEventsConsistentWithResult;
import static com.hazelcast.partition.PartitionMigrationListenerTest.assertMigrationProcessCompleted;
import static com.hazelcast.partition.PartitionMigrationListenerTest.assertMigrationProcessEventsConsistent;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static com.hazelcast.spi.properties.ClusterProperty.SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED;

@RunWith(HazelcastSerialClassRunner.class)
@Category({NightlyTest.class})
public class PartitionMigrationComputeIntensiveTest extends HazelcastTestSupport {

    private static final ILogger LOGGER = Logger.getLogger(PartitionMigrationComputeIntensiveTest.class);

    @Test
    public void testMigrationStats_afterPartitionsLost_when_NO_MIGRATION() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        Config config = new Config().setProperty(PARTITION_COUNT.getName(), "2000")
                                    .setProperty(SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED.getName(), "true");
        HazelcastInstance[] instances = factory.newInstances(config, 10);

        assertClusterSizeEventually(instances.length, instances);

        LOGGER.info("Cluster formed and in safe state, warming up partitions...");
        warmUpPartitions(instances);

        LOGGER.info("Adding migration listener");
        EventCollectingMigrationListener listener = new EventCollectingMigrationListener(false);
        instances[0].getPartitionService().addMigrationListener(listener);

        LOGGER.info("Changing cluster state to PASSIVE");
        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        for (int i = 3; i < instances.length; i++) {
            instances[i].getLifecycleService().terminate();
        }

        LOGGER.info("Changing cluster state to NO_MIGRATION");
        changeClusterStateEventually(instances[0], ClusterState.NO_MIGRATION);

        // 3 promotions on each remaining node + 1 to assign owners for lost partitions
        for (MigrationEventsPack eventsPack : listener.ensureAndGetEventPacks(4)) {
            assertMigrationProcessCompleted(eventsPack);
            assertMigrationProcessEventsConsistent(eventsPack);
            assertMigrationEventsConsistentWithResult(eventsPack);
        }
    }
}
