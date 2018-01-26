/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cluster.ClusterState.ACTIVE;
import static com.hazelcast.cluster.ClusterState.PASSIVE;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.eviction.ExpirationManager.PROP_CLEANUP_OPERATION_COUNT;
import static com.hazelcast.map.impl.eviction.ExpirationManager.PROP_CLEANUP_PERCENTAGE;
import static com.hazelcast.map.impl.eviction.ExpirationManager.PROP_PRIMARY_DRIVES_BACKUP;
import static com.hazelcast.map.impl.eviction.ExpirationManager.PROP_TASK_PERIOD_SECONDS;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.lang.System.clearProperty;
import static java.lang.System.getProperty;
import static java.lang.System.setProperty;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ExpirationManagerTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testTaskPeriodSeconds_set_viaSystemProperty() throws Exception {
        String previous = getProperty(PROP_TASK_PERIOD_SECONDS);
        try {
            int expectedPeriodSeconds = 12;
            setProperty(PROP_TASK_PERIOD_SECONDS, valueOf(expectedPeriodSeconds));

            int actualTaskPeriodSeconds = newExpirationManager(createHazelcastInstance()).getTaskPeriodSeconds();

            assertEquals(expectedPeriodSeconds, actualTaskPeriodSeconds);
        } finally {
            restoreProperty(PROP_TASK_PERIOD_SECONDS, previous);

        }
    }

    @Test
    public void testPrimaryDrivesEvictions_set_viaSystemProperty() throws Exception {
        String previous = getProperty(PROP_PRIMARY_DRIVES_BACKUP);
        try {
            setProperty(PROP_PRIMARY_DRIVES_BACKUP, "False");

            boolean primaryDrivesEviction = newExpirationManager(createHazelcastInstance()).isPrimaryDrivesEviction();

            assertEquals(false, primaryDrivesEviction);
        } finally {
            restoreProperty(PROP_PRIMARY_DRIVES_BACKUP, previous);
        }
    }

    private void restoreProperty(String sysProp, String previous) {
        if (previous == null) {
            clearProperty(sysProp);
        } else {
            setProperty(sysProp, previous);
        }
    }

    @Test
    public void testTaskPeriodSeconds_throwsIllegalArgumentException_whenNotPositive() throws Exception {
        String previous = getProperty(PROP_TASK_PERIOD_SECONDS);
        try {
            setProperty(PROP_TASK_PERIOD_SECONDS, valueOf(0));

            thrown.expectMessage("taskPeriodSeconds should be a positive number");
            thrown.expect(IllegalArgumentException.class);

            newExpirationManager(createHazelcastInstance());
        } finally {
            restoreProperty(PROP_TASK_PERIOD_SECONDS, previous);
        }
    }

    @Test
    public void testCleanupPercentage_set_viaSystemProperty() throws Exception {
        String previous = getProperty(PROP_CLEANUP_PERCENTAGE);
        try {
            int expectedCleanupPercentage = 77;
            setProperty(PROP_CLEANUP_PERCENTAGE, valueOf(expectedCleanupPercentage));

            int actualCleanupPercentage = newExpirationManager(createHazelcastInstance()).getCleanupPercentage();

            assertEquals(expectedCleanupPercentage, actualCleanupPercentage);
        } finally {
            restoreProperty(PROP_CLEANUP_PERCENTAGE, previous);
        }
    }

    @Test
    public void testCleanupPercentage_throwsIllegalArgumentException_whenNotInRange() throws Exception {
        String previous = getProperty(PROP_CLEANUP_PERCENTAGE);
        try {
            setProperty(PROP_CLEANUP_PERCENTAGE, valueOf(0));

            thrown.expectMessage("cleanupPercentage should be in range (0,100]");
            thrown.expect(IllegalArgumentException.class);

            newExpirationManager(createHazelcastInstance());
        } finally {
            restoreProperty(PROP_CLEANUP_PERCENTAGE, previous);
        }
    }

    @Test
    public void testCleanupOperationCount_set_viaSystemProperty() throws Exception {
        String previous = getProperty(PROP_CLEANUP_OPERATION_COUNT);
        try {
            int expectedCleanupOperationCount = 19;
            setProperty(PROP_CLEANUP_OPERATION_COUNT, valueOf(expectedCleanupOperationCount));

            int actualCleanupOperationCount = newExpirationManager(createHazelcastInstance()).getCleanupOperationCount();

            assertEquals(expectedCleanupOperationCount, actualCleanupOperationCount);
        } finally {
            restoreProperty(PROP_CLEANUP_OPERATION_COUNT, previous);
        }
    }

    @Test
    public void testCleanupOperationCount_throwsIllegalArgumentException_whenNotPositive() throws Exception {
        String previous = getProperty(PROP_CLEANUP_OPERATION_COUNT);
        try {
            setProperty(PROP_CLEANUP_OPERATION_COUNT, valueOf(0));

            thrown.expectMessage("cleanupOperationCount should be a positive number");
            thrown.expect(IllegalArgumentException.class);

            newExpirationManager(createHazelcastInstance());
        } finally {
            restoreProperty(PROP_CLEANUP_OPERATION_COUNT, previous);
        }
    }

    @Test
    public void gets_taskPeriodSeconds_from_config() throws Exception {
        Config config = new Config();
        String taskPeriodSeconds = "77";
        config.setProperty(PROP_TASK_PERIOD_SECONDS, taskPeriodSeconds);
        HazelcastInstance node = createHazelcastInstance(config);
        ExpirationManager expirationManager = newExpirationManager(node);

        assertEquals(parseInt(taskPeriodSeconds), expirationManager.getTaskPeriodSeconds());
    }

    @Test
    public void gets_cleanupPercentage_from_config() throws Exception {
        Config config = new Config();
        String cleanupPercentage = "99";
        config.setProperty(PROP_CLEANUP_PERCENTAGE, cleanupPercentage);
        HazelcastInstance node = createHazelcastInstance(config);
        ExpirationManager expirationManager = newExpirationManager(node);

        assertEquals(parseInt(cleanupPercentage), expirationManager.getCleanupPercentage());
    }

    @Test
    public void gets_cleanupOperationCount_from_config() throws Exception {
        Config config = new Config();
        String cleanupOperationCount = "777";
        config.setProperty(PROP_CLEANUP_OPERATION_COUNT, cleanupOperationCount);
        HazelcastInstance node = createHazelcastInstance(config);
        ExpirationManager expirationManager = newExpirationManager(node);

        assertEquals(parseInt(cleanupOperationCount), expirationManager.getCleanupOperationCount());
    }

    @Test
    public void stops_running_when_clusterState_turns_passive() throws Exception {
        Config config = new Config();
        config.setProperty(PROP_TASK_PERIOD_SECONDS, "1");
        HazelcastInstance node = createHazelcastInstance(config);

        final AtomicInteger expirationCounter = new AtomicInteger();

        IMap map = node.getMap("test");
        map.addEntryListener(new EntryExpiredListener() {
            @Override
            public void entryExpired(EntryEvent event) {
                expirationCounter.incrementAndGet();
            }
        }, true);

        map.put(1, 1, 3, TimeUnit.SECONDS);

        node.getCluster().changeClusterState(PASSIVE);

        // wait a little to see if any expiration is occurring
        sleepSeconds(3);

        int expirationCount = expirationCounter.get();
        assertEquals(format("Expecting no expiration but found:%d", expirationCount), 0, expirationCount);
    }

    @Test
    public void starts_running_when_clusterState_turns_active() throws Exception {
        Config config = new Config();
        config.setProperty(PROP_TASK_PERIOD_SECONDS, "1");
        HazelcastInstance node = createHazelcastInstance(config);

        final AtomicInteger expirationCounter = new AtomicInteger();

        IMap map = node.getMap("test");
        map.addEntryListener(new EntryExpiredListener() {
            @Override
            public void entryExpired(EntryEvent event) {
                expirationCounter.incrementAndGet();
            }
        }, true);

        map.put(1, 1, 3, SECONDS);

        node.getCluster().changeClusterState(PASSIVE);
        node.getCluster().changeClusterState(ACTIVE);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int expirationCount = expirationCounter.get();
                assertEquals(format("Expecting 1 expiration but found:%d", expirationCount), 1, expirationCount);
            }
        });
    }

    private ExpirationManager newExpirationManager(HazelcastInstance node) {
        return new ExpirationManager(getPartitionContainers(node), getNodeEngineImpl(node));
    }

    private PartitionContainer[] getPartitionContainers(HazelcastInstance instance) {
        return ((MapService) getNodeEngineImpl(instance).getService(SERVICE_NAME)).getMapServiceContext().getPartitionContainers();
    }
}
