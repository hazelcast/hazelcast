/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatchers;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static com.hazelcast.instance.impl.ClusterTopologyIntent.CLUSTER_SHUTDOWN;
import static com.hazelcast.instance.impl.ClusterTopologyIntent.CLUSTER_STABLE;
import static com.hazelcast.instance.impl.ClusterTopologyIntent.CLUSTER_START;
import static com.hazelcast.instance.impl.ClusterTopologyIntent.IN_MANAGED_CONTEXT_UNKNOWN;
import static com.hazelcast.instance.impl.ClusterTopologyIntent.NOT_IN_MANAGED_CONTEXT;
import static com.hazelcast.instance.impl.ClusterTopologyIntentTracker.UNKNOWN;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@QuickTest
@ParallelJVMTest
public class KubernetesTopologyIntentTrackerTest {

    private final Properties properties = new Properties();
    private final ClusterServiceImpl clusterService = mock(ClusterServiceImpl.class);
    private KubernetesTopologyIntentTracker clusterTopologyIntentTracker;

    private Node setupMockNode() {
        Node node = mock(Node.class);
        properties.setProperty(ClusterProperty.CLUSTER_SHUTDOWN_TIMEOUT_SECONDS.getName(), "2");
        HazelcastProperties hazelcastProperties = new HazelcastProperties(properties);
        when(node.getProperties()).thenReturn(hazelcastProperties);
        when(node.getLogger(ArgumentMatchers.any(Class.class)))
                .thenReturn(Logger.getLogger(KubernetesTopologyIntentTracker.class));
        when(node.getClusterService()).thenReturn(clusterService);
        return node;
    }

    @Test
    public void testConstructor_whenClusterAutoStateStrategyActive() {
        properties.setProperty(ClusterProperty.PERSISTENCE_AUTO_CLUSTER_STATE_STRATEGY.getName(), "ACTIVE");
        Node node = setupMockNode();
        assertThrows(InvalidConfigurationException.class, () -> new KubernetesTopologyIntentTracker(node));
    }

    @Test
    public void testConstructor_whenInvalidClusterAutoStateStrategy() {
        properties.setProperty(ClusterProperty.PERSISTENCE_AUTO_CLUSTER_STATE_STRATEGY.getName(), "NOT_A_CLUSTER_STATE");
        Node node = setupMockNode();

        assertThrows(IllegalArgumentException.class,
                () -> new KubernetesTopologyIntentTracker(node));
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
        Future<?> future = spawn(() -> {
            clusterTopologyIntentTracker.waitForMissingMember();
        });
        sleepSeconds(1);
        // trigger membership change event to update size of cluster from clusterService
        clusterTopologyIntentTracker.onMembershipChange();
        // ensure wait thread completed
        future.get(15, TimeUnit.SECONDS);
    }

    @ParameterizedTest(name = "{index}: prevSpec={0}, updatedSpec={1}, "
            + "prevReady={2}, updatedReady={3}, "
            + "prevCurrent={4}, updatedCurrent={5}, "
            + "expected={6}")
    @MethodSource("updateCases")
    public void testClusterTopologyIntentUpdate(
            int p1, int p2, int p3, int p4, int p5, int p6,
            ClusterTopologyIntent expectedIntent
    ) {
        clusterTopologyIntentTracker = new KubernetesTopologyIntentTracker(setupMockNode());
        clusterTopologyIntentTracker.update(p1, p2, p3, p4, p5, p6);
        assertThat(clusterTopologyIntentTracker.getClusterTopologyIntent())
                .isEqualTo(expectedIntent);
    }

    static Stream<Arguments> updateCases() {
        return Stream.of(
                // Initial unknown -> handleInitialUpdate.
                Arguments.of(
                        UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, IN_MANAGED_CONTEXT_UNKNOWN
                ),
                Arguments.of(
                        UNKNOWN, UNKNOWN, UNKNOWN, 0, UNKNOWN, 0, IN_MANAGED_CONTEXT_UNKNOWN
                ),
                // Initial unknown, but some replicas still unknown (first update)
                Arguments.of(
                        UNKNOWN, 1, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, CLUSTER_START
                ),
                Arguments.of(
                        UNKNOWN, 1, UNKNOWN, 0, UNKNOWN, 0, CLUSTER_START
                ),
                // updated replica = 0 -> full cluster shutdown
                Arguments.of(
                        1, 0, 1, UNKNOWN, 1, UNKNOWN, CLUSTER_SHUTDOWN
                ),
                // previous Replicas == updated Replicas
                Arguments.of(
                        1, 1, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, NOT_IN_MANAGED_CONTEXT
                ),
                Arguments.of(
                        1, 1, UNKNOWN, 0, UNKNOWN, 0, NOT_IN_MANAGED_CONTEXT
                ),
                Arguments.of(
                        1, 1, 0, UNKNOWN, 0, UNKNOWN, NOT_IN_MANAGED_CONTEXT
                ),
                Arguments.of(
                        1, 1, 1, UNKNOWN, 1, UNKNOWN, NOT_IN_MANAGED_CONTEXT
                ),
                Arguments.of(
                        1, 1, 1, 0, 1, 0, NOT_IN_MANAGED_CONTEXT
                ),
                Arguments.of(
                        1, 1, UNKNOWN, 1, UNKNOWN, 1, CLUSTER_STABLE
                ),
                Arguments.of(
                        1, 1, 0, 1, 0, 1, CLUSTER_STABLE
                ),
                // Stable cluster (spec equal, ready == spec)
                Arguments.of(
                        3, 3, 2, 3, 2, 3, ClusterTopologyIntent.CLUSTER_STABLE
                ),
                // scaling
                Arguments.of(
                        2, 3, 2, UNKNOWN, 2, UNKNOWN, ClusterTopologyIntent.SCALING
                ),
                Arguments.of(
                        2, 3, 2, 0, 2, 0, ClusterTopologyIntent.SCALING
                )
        );
    }
}
