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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MCReadMetricsCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Test to verify the FIX for memory leak issue #26463.
 *
 * Original bug scenario:
 * 1. Client requests metrics with sequence == tail
 * 2. ConcurrentArrayRingbuffer.copyFrom() returns EMPTY slice
 * 3. MetricsService.tryCompleteRead() used to skip future.complete() for empty slices
 * 4. Future never completes → doSendResponse() never called → deregister() never called
 * 5. Operation leaks in LiveOperationRegistry
 *
 * Fix:
 * - MetricsService.tryCompleteRead() now ALWAYS calls future.complete(), even with empty slice
 * - Added timeout mechanism as safety net (60 seconds)
 * - Empty slice is a valid response meaning "no data available yet"
 *
 * This test verifies the fix works correctly.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class ReadMetricsOperationMemoryLeakImprovedTest extends ClientTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastInstance member;
    private HazelcastInstance client;
    private HazelcastClientInstanceImpl clientImpl;
    private NodeEngineImpl nodeEngine;
    private MetricsService metricsService;
    private UUID memberUuid;

    @Before
    public void setUp() {
        // Create Hazelcast member with metrics enabled
        // Use VERY SLOW collection to simulate the problem
        Config config = new Config();
        config.getMetricsConfig().setEnabled(true);
        config.getMetricsConfig().setCollectionFrequencySeconds(3600); // 1 hour! Very slow
        config.getMetricsConfig().getManagementCenterConfig().setEnabled(true);
        config.getMetricsConfig().getManagementCenterConfig().setRetentionSeconds(60); // Very small - only 1 minute

        member = factory.newHazelcastInstance(config);
        client = factory.newHazelcastClient();

        clientImpl = getHazelcastClientInstanceImpl(client);
        nodeEngine = getNode(member).getNodeEngine();
        metricsService = nodeEngine.getService(MetricsService.SERVICE_NAME);
        memberUuid = member.getCluster().getLocalMember().getUuid();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    /**
     * This test verifies the FIX for issue #26463.
     *
     * Expected behavior: NO LEAK - operations are properly cleaned up even when requesting sequence == tail
     */
    @Test
    public void testMemoryLeak_whenRequestingFutureSequenceWithinRetention() throws Exception {
        // Force initial metrics collection
        forceMetricsCollection();
        sleepSeconds(1);

        // Get current state
        int initialSize = getLiveOperationRegistrySize();

        // Get the tail sequence (nextSequence - the sequence that hasn't been written yet)
        long tailSequence = getCurrentTailSequence();

        // THIS IS THE BUG SCENARIO FROM ISSUE #26463:
        // Client requests metrics with sequence == tail
        // Before the fix: future never completes → operation leaks in LiveOperationRegistry
        // After the fix: future.complete() is called even for empty slice → operation is cleaned up

        // Send request using client with sequence == tail (as described in the issue)
        ClientMessage request = MCReadMetricsCodec.encodeRequest(memberUuid, tailSequence);
        ClientInvocation invocation = new ClientInvocation(clientImpl, request, null);

        // Try to get with short timeout - should complete with empty slice or timeout
        try {
            invocation.invoke().get(500, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // Expected - operation is waiting for future data
        }

        // Wait for operations to settle
        sleepSeconds(2);

        // Check if operations leaked on the member side
        int afterSize = getLiveOperationRegistrySize();
        int leakedOperations = afterSize - initialSize;

        // Verify the FIX - operations should be cleaned up now!
        if (leakedOperations != 0) {
            printLiveOperationRegistryState();
            fail("Fix verification failed: " + leakedOperations + " operations leaked. "
                + "Expected 0 leaked operations after the fix.");
        }
    }

    /**
     * Test to verify that when metrics ARE collected, no leak occurs.
     * This is the control test.
     */
    @Test
    public void testNoLeak_whenMetricsAreCollected() throws Exception {
        // Force multiple metrics collections to ensure we have data
        for (int i = 0; i < 3; i++) {
            forceMetricsCollection();
            sleepSeconds(1);
        }

        int initialSize = getLiveOperationRegistrySize();

        // Request metrics from sequence 0 using client (should have data now)
        ClientMessage request = MCReadMetricsCodec.encodeRequest(memberUuid, 0);
        ClientInvocation invocation = new ClientInvocation(clientImpl, request, null);

        ClientMessage response = invocation.invoke().get(5, TimeUnit.SECONDS);
        MCReadMetricsCodec.ResponseParameters params = MCReadMetricsCodec.decodeResponse(response);
        assertFalse("Expected at least one metric entry", params.elements.isEmpty());

        sleepSeconds(1);

        int afterSize = getLiveOperationRegistrySize();

        if (afterSize != initialSize) {
            fail("Unexpected leak in control test: " + (afterSize - initialSize) + " operations leaked");
        }
    }

    // ==================== Helper Methods ====================

    /**
     * Get the current tail sequence (nextSequence - the next sequence that hasn't been written yet).
     * This is used to reproduce the bug scenario where client requests metrics with sequence == tail.
     */
    private long getCurrentTailSequence() throws Exception {
        // Read from sequence 0 and get nextSequence (tail)
        try {
            ClientMessage request = MCReadMetricsCodec.encodeRequest(memberUuid, 0);
            ClientInvocation invocation = new ClientInvocation(clientImpl, request, null);
            ClientMessage response = invocation.invoke().get(2, TimeUnit.SECONDS);
            MCReadMetricsCodec.ResponseParameters params = MCReadMetricsCodec.decodeResponse(response);
            // nextSequence is the tail - the next sequence that hasn't been written yet
            return params.nextSequence;
        } catch (Exception e) {
            // If failed, assume tail is 0
            return 0;
        }
    }

    private void forceMetricsCollection() {
        // Trigger metrics collection manually
        try {
            metricsService.collectMetrics();
        } catch (Exception e) {
            // Ignore - metrics collection may fail if not fully initialized
        }
    }

    private int getLiveOperationRegistrySize() throws Exception {
        return getLiveOperationRegistrySize(metricsService);
    }

    private int getLiveOperationRegistrySize(MetricsService service) throws Exception {
        LiveOperationRegistry registry = service.getLiveOperationRegistry();

        Field liveOperationsField = LiveOperationRegistry.class.getDeclaredField("liveOperations");
        liveOperationsField.setAccessible(true);

        @SuppressWarnings("unchecked")
        ConcurrentHashMap<?, Map<Long, ?>> liveOperations =
            (ConcurrentHashMap<?, Map<Long, ?>>) liveOperationsField.get(registry);

        int totalOperations = 0;
        for (Map<Long, ?> addressOps : liveOperations.values()) {
            totalOperations += addressOps.size();
        }

        return totalOperations;
    }

    private void printLiveOperationRegistryState() throws Exception {
        // Diagnostic method for debugging - called only on test failure
        LiveOperationRegistry registry = metricsService.getLiveOperationRegistry();
        Field liveOperationsField = LiveOperationRegistry.class.getDeclaredField("liveOperations");
        liveOperationsField.setAccessible(true);

        @SuppressWarnings("unchecked")
        ConcurrentHashMap<?, Map<Long, ?>> liveOperations =
            (ConcurrentHashMap<?, Map<Long, ?>>) liveOperationsField.get(registry);

        // Information will be available in test failure output
        int totalOps = 0;
        for (Map<Long, ?> ops : liveOperations.values()) {
            totalOps += ops.size();
        }

        // This will appear in test logs when the test fails
        if (totalOps > 0) {
            fail("Found " + totalOps + " leaked operations across " + liveOperations.size() + " addresses");
        }
    }
}
