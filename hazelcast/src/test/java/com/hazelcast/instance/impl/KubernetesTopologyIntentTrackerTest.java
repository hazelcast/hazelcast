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

package com.hazelcast.instance.impl;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.instance.impl.ClusterTopologyIntentTracker.UNKNOWN;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class KubernetesTopologyIntentTrackerTest {

    private final Properties properties = new Properties();
    private final ClusterServiceImpl clusterService = mock(ClusterServiceImpl.class);
    private KubernetesTopologyIntentTracker clusterTopologyIntentTracker;

    private Node setupMockNode() {
        Node node = mock(Node.class);
        properties.put(ClusterProperty.CLUSTER_SHUTDOWN_TIMEOUT_SECONDS.getName(), "2");
        HazelcastProperties hazelcastProperties = new HazelcastProperties(properties);
        when(node.getProperties()).thenReturn(hazelcastProperties);
        when(node.getLogger(ArgumentMatchers.any(Class.class)))
                .thenReturn(Logger.getLogger(KubernetesTopologyIntentTracker.class));
        when(node.getClusterService()).thenReturn(clusterService);
        return node;
    }

    @Test
    public void testConstructor_whenClusterAutoStateStrategyActive() {
        properties.put(ClusterProperty.PERSISTENCE_AUTO_CLUSTER_STATE_STRATEGY.getName(), "ACTIVE");
        assertThrows(InvalidConfigurationException.class,
                () -> new KubernetesTopologyIntentTracker(setupMockNode()));
    }

    @Test
    public void testConstructor_whenInvalidClusterAutoStateStrategy() {
        properties.put(ClusterProperty.PERSISTENCE_AUTO_CLUSTER_STATE_STRATEGY.getName(), "NOT_A_CLUSTER_STATE");
        assertThrows(IllegalArgumentException.class,
                () -> new KubernetesTopologyIntentTracker(setupMockNode()));
    }

    @Test
    public void test_waitCallableWithTimeout_whenImmediatelyTrue() {
        clusterTopologyIntentTracker = new KubernetesTopologyIntentTracker(setupMockNode());
        // wait 100 seconds for condition to be true
        long timeRemaining = clusterTopologyIntentTracker.waitCallableWithTimeout(() -> true,
                TimeUnit.SECONDS.toNanos(100));
        System.out.println(">> timeRemaining " + timeRemaining);
        // assert at least 95 seconds remain from given timeout (to avoid spurious failures on busy CI machine)
        assertTrue(timeRemaining > TimeUnit.SECONDS.toNanos(95));
    }

    @Test
    public void test_waitCallableWithTimeout_whenAlwaysFalse() {
        clusterTopologyIntentTracker = new KubernetesTopologyIntentTracker(setupMockNode());
        long timeRemaining = clusterTopologyIntentTracker.waitCallableWithTimeout(() -> true,
                TimeUnit.SECONDS.toNanos(100));
        assertTrue(timeRemaining > TimeUnit.SECONDS.toNanos(95));
    }

    @Test
    public void test_waitForMissingMembers() throws InterruptedException, ExecutionException, TimeoutException {
        when(clusterService.getSize()).thenReturn(3);
        clusterTopologyIntentTracker = new KubernetesTopologyIntentTracker(setupMockNode());
        // cluster is running with 3 members
        clusterTopologyIntentTracker.update(UNKNOWN, 3,
                UNKNOWN, 3, UNKNOWN, 3);
        // cluster is missing a member
        clusterTopologyIntentTracker.update(3, 3,
                3, 2,
                3, 2);
        // cluster shutdown starts
        clusterTopologyIntentTracker.update(3, 0,
                2, 3,
                2, 2);
        Future future = spawn(() -> {
            clusterTopologyIntentTracker.waitForMissingMember();
        });
        sleepSeconds(1);
        // trigger membership change event to update size of cluster from clusterService
        clusterTopologyIntentTracker.onMembershipChange();
        // ensure wait thread completed
        future.get(15, TimeUnit.SECONDS);
    }
}
