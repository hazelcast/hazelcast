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

package com.hazelcast.internal.eviction;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cluster.ClusterState.ACTIVE;
import static com.hazelcast.cluster.ClusterState.PASSIVE;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.lang.System.clearProperty;
import static java.lang.System.getProperty;
import static java.lang.System.setProperty;
import static org.junit.Assert.assertEquals;

public abstract class AbstractExpirationManagerTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testTaskPeriodSeconds_set_viaSystemProperty() {
        String previous = getProperty(taskPeriodSecondsPropName());
        try {
            int expectedPeriodSeconds = 12;
            setProperty(taskPeriodSecondsPropName(), valueOf(expectedPeriodSeconds));

            int actualTaskPeriodSeconds = newExpirationManager(createHazelcastInstance()).getTaskPeriodSeconds();

            assertEquals(expectedPeriodSeconds, actualTaskPeriodSeconds);
        } finally {
            restoreProperty(taskPeriodSecondsPropName(), previous);
        }
    }

    @Test
    public void testTaskPeriodSeconds_throwsIllegalArgumentException_whenNotPositive() throws Exception {
        String previous = getProperty(taskPeriodSecondsPropName());
        try {
            setProperty(taskPeriodSecondsPropName(), valueOf(0));

            thrown.expectMessage("taskPeriodSeconds is 0 but must be > 0");
            thrown.expect(IllegalArgumentException.class);

            newExpirationManager(createHazelcastInstance());
        } finally {
            restoreProperty(taskPeriodSecondsPropName(), previous);
        }
    }

    @Test
    public void testCleanupPercentage_set_viaSystemProperty() {
        String previous = getProperty(cleanupPercentagePropName());
        try {
            int expectedCleanupPercentage = 77;
            setProperty(cleanupPercentagePropName(), valueOf(expectedCleanupPercentage));

            int actualCleanupPercentage = getCleanupPercentage(newExpirationManager(createHazelcastInstance()));

            assertEquals(expectedCleanupPercentage, actualCleanupPercentage);
        } finally {
            restoreProperty(cleanupPercentagePropName(), previous);
        }
    }

    @Test
    public void testCleanupPercentage_throwsIllegalArgumentException_whenUnderRange() {
        String previous = getProperty(cleanupPercentagePropName());
        try {
            setProperty(cleanupPercentagePropName(), valueOf(0));

            thrown.expectMessage("cleanupPercentage should be in range (0,100]");
            thrown.expect(IllegalArgumentException.class);

            newExpirationManager(createHazelcastInstance());
        } finally {
            restoreProperty(cleanupPercentagePropName(), previous);
        }
    }

    @Test
    public void testCleanupPercentage_throwsIllegalArgumentException_whenAboveRange() {
        String previous = getProperty(cleanupPercentagePropName());
        try {
            setProperty(cleanupPercentagePropName(), valueOf(101));

            thrown.expectMessage("cleanupPercentage should be in range (0,100]");
            thrown.expect(IllegalArgumentException.class);

            newExpirationManager(createHazelcastInstance());
        } finally {
            restoreProperty(cleanupPercentagePropName(), previous);
        }
    }

    @Test
    public void testCleanupOperationCount_set_viaSystemProperty() {
        String previous = getProperty(cleanupOperationCountPropName());
        try {
            int expectedCleanupOperationCount = 19;
            setProperty(cleanupOperationCountPropName(), valueOf(expectedCleanupOperationCount));

            int actualCleanupOperationCount = getCleanupOperationCount(newExpirationManager(createHazelcastInstance()));

            assertEquals(expectedCleanupOperationCount, actualCleanupOperationCount);
        } finally {
            restoreProperty(cleanupOperationCountPropName(), previous);
        }
    }

    @Test
    public void testCleanupOperationCount_throwsIllegalArgumentException_whenNotPositive() {
        String previous = getProperty(cleanupOperationCountPropName());
        try {
            setProperty(cleanupOperationCountPropName(), valueOf(0));

            thrown.expectMessage("cleanupOperationCount should be a positive number");
            thrown.expect(IllegalArgumentException.class);

            newExpirationManager(createHazelcastInstance());
        } finally {
            restoreProperty(cleanupOperationCountPropName(), previous);
        }
    }

    @Test
    public void gets_taskPeriodSeconds_from_config() {
        Config config = getConfig();
        String taskPeriodSeconds = "77";
        config.setProperty(taskPeriodSecondsPropName(), taskPeriodSeconds);
        HazelcastInstance node = createHazelcastInstance(config);
        ExpirationManager expirationManager = newExpirationManager(node);

        assertEquals(parseInt(taskPeriodSeconds), expirationManager.getTaskPeriodSeconds());
    }

    @Test
    public void gets_cleanupPercentage_from_config() {
        Config config = getConfig();
        String cleanupPercentage = "99";
        config.setProperty(cleanupPercentagePropName(), cleanupPercentage);
        HazelcastInstance node = createHazelcastInstance(config);
        ExpirationManager expirationManager = newExpirationManager(node);

        assertEquals(parseInt(cleanupPercentage), getCleanupPercentage(expirationManager));
    }

    @Test
    public void gets_cleanupOperationCount_from_config() {
        Config config = getConfig();
        String cleanupOperationCount = "777";
        config.setProperty(cleanupOperationCountPropName(), cleanupOperationCount);
        HazelcastInstance node = createHazelcastInstance(config);
        ExpirationManager expirationManager = newExpirationManager(node);

        assertEquals(parseInt(cleanupOperationCount), getCleanupOperationCount(expirationManager));
    }

    @Test
    public void stops_running_when_clusterState_turns_passive() {
        Config config = getConfig();
        config.setProperty(taskPeriodSecondsPropName(), "1");
        HazelcastInstance node = createHazelcastInstance(config);

        final AtomicInteger expirationCounter = configureForTurnsActivePassiveTest(node);

        node.getCluster().changeClusterState(PASSIVE);

        // wait a little to see if any expiration is occurring
        sleepSeconds(3);

        int expirationCount = expirationCounter.get();
        assertEquals(format("Expecting no expiration but found:%d", expirationCount), 0, expirationCount);
    }

    @Test
    public void starts_running_when_clusterState_turns_active() {
        Config config = getConfig();
        config.setProperty(taskPeriodSecondsPropName(), "1");
        HazelcastInstance node = createHazelcastInstance(config);

        final AtomicInteger expirationCounter = configureForTurnsActivePassiveTest(node);

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

    protected int getCleanupOperationCount(ExpirationManager expirationManager) {
        return expirationManager.getCleanupOperationCount();
    }

    protected int getCleanupPercentage(ExpirationManager expirationManager) {
        return expirationManager.getCleanupPercentage();
    }

    protected void restoreProperty(String sysProp, String previous) {
        if (previous == null) {
            clearProperty(sysProp);
        } else {
            setProperty(sysProp, previous);
        }
    }

    protected abstract ExpirationManager newExpirationManager(HazelcastInstance node);

    protected abstract String cleanupOperationCountPropName();

    protected abstract String taskPeriodSecondsPropName();

    protected abstract String cleanupPercentagePropName();

    protected abstract String cleanupTaskEnabledPropName();

    protected abstract AtomicInteger configureForTurnsActivePassiveTest(HazelcastInstance node);
}
