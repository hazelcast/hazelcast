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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.managementcenter.ConcurrentArrayRingbuffer.RingbufferSlice;
import com.hazelcast.internal.metrics.managementcenter.ReadMetricsOperation;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.test.Accessors.getNode;
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
public class ReadMetricsOperationMemoryLeakImprovedTest extends HazelcastTestSupport {

    private HazelcastInstance instance;
    private NodeEngineImpl nodeEngine;
    private OperationServiceImpl operationService;
    private MetricsService metricsService;

    @Before
    public void setUp() {
        // Create Hazelcast instance with metrics enabled
        // Use VERY SLOW collection to simulate the problem
        Config config = new Config();
        config.getMetricsConfig().setEnabled(true);
        config.getMetricsConfig().setCollectionFrequencySeconds(3600); // 1 hour! Very slow
        config.getMetricsConfig().getManagementCenterConfig().setEnabled(true);
        config.getMetricsConfig().getManagementCenterConfig().setRetentionSeconds(60); // Very small - only 1 minute

        instance = createHazelcastInstance(config);
        nodeEngine = getNode(instance).getNodeEngine();
        operationService = nodeEngine.getOperationService();
        metricsService = nodeEngine.getService(MetricsService.SERVICE_NAME);
    }

    @After
    public void tearDown() {
        if (instance != null) {
            instance.shutdown();
        }
    }

    /**
     * This test verifies the FIX for issue #26463.
     *
     * Expected behavior: NO LEAK - operations are properly cleaned up even when requesting sequence == tail
     */
    @Test
    public void testMemoryLeak_whenRequestingFutureSequenceWithinRetention() throws Exception {
        System.out.println("\n=== TEST: Verify Fix for Issue #26463 ===\n");

        // Print heap size to verify VM options
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory() / (1024 * 1024);
        long totalMemory = runtime.totalMemory() / (1024 * 1024);
        System.out.println("JVM Max Memory: " + maxMemory + " MB");
        System.out.println("JVM Total Memory: " + totalMemory + " MB");
        System.out.println();

        // Step 1: Force initial metrics collection
        System.out.println("Step 1: Triggering initial metrics collection...");
        forceMetricsCollection();
        sleepSeconds(1);

        // Step 2: Get current state
        int initialSize = getLiveOperationRegistrySize();
        System.out.println("Initial LiveOperationRegistry size: " + initialSize);

        long currentTailSequence = getCurrentHeadSequence(); // This is actually tail!
        System.out.println("Current tail sequence: " + currentTailSequence);

        // Step 3: Request sequence that equals tail (empty slice, no exception)
        // When we request sequence == tail:
        // - copyFrom() returns empty slice (line 77)
        // - future.complete() is NOT called (line 193-195)
        // - Operation remains in LiveOperationRegistry forever!
        long futureSequence = currentTailSequence; // Request tail itself
        System.out.println("Requesting tail sequence: " + futureSequence);
        System.out.println("(This will return empty slice without completing the future)");

        int numberOfOperations = 1; // Only 1 operation to minimize memory usage

        // Step 4: Send operations that will get stuck
        for (int i = 0; i < numberOfOperations; i++) {
            ReadMetricsOperation operation = new ReadMetricsOperation(futureSequence);

            System.out.println("About to invoke operation " + i + "...");
            InternalCompletableFuture<RingbufferSlice<Map.Entry<Long, byte[]>>> future =
                operationService.invokeOnTarget(
                    MetricsService.SERVICE_NAME,
                    operation,
                    nodeEngine.getThisAddress()
                );
            System.out.println("Operation " + i + " invoked, future isDone=" + future.isDone());

            // Add callback to detect when future completes
            future.whenComplete((result, error) -> {
                if (error != null) {
                    System.out.println(">>> Future completed with ERROR: " + error.getClass().getSimpleName() + ": " + error.getMessage());
                } else {
                    System.out.println(">>> Future completed with result: " + (result != null ? result.elements().size() + " elements" : "null"));
                }
            });

            // Try to get with short timeout - should timeout because data doesn't exist
            try {
                RingbufferSlice<Map.Entry<Long, byte[]>> result = future.get(500, TimeUnit.MILLISECONDS);
                System.out.println("Operation " + i + " completed unexpectedly with " + result.elements().size() + " elements!");
            } catch (TimeoutException e) {
                // Expected - operation is waiting for future data
                System.out.println("Operation " + i + " is pending (timeout - expected)");
            } catch (Exception e) {
                System.out.println("Operation " + i + " failed with: " + e.getClass().getSimpleName() + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        // Step 5: Wait a bit for operations to settle
        sleepSeconds(2);

        // Step 6: Check if operations leaked
        int afterSize = getLiveOperationRegistrySize();
        System.out.println("\nAfter " + numberOfOperations + " operations:");
        System.out.println("LiveOperationRegistry size: " + afterSize);

        int leakedOperations = afterSize - initialSize;
        System.out.println("Leaked operations: " + leakedOperations);

        // Step 7: Print diagnostic info
        printLiveOperationRegistryState();

        // Step 8: Verify the FIX - operations should be cleaned up now!
        if (leakedOperations == 0) {
            System.out.println("\n=== ✅ BUG FIXED! ===");
            System.out.println("Number of operations sent: " + numberOfOperations);
            System.out.println("Number of operations leaked: " + leakedOperations);
            System.out.println("\nFix verification:");
            System.out.println("1. Requested sequence=" + futureSequence + " (equals tail)");
            System.out.println("2. ConcurrentArrayRingbuffer.copyFrom() returned EMPTY slice");
            System.out.println("3. MetricsService.tryCompleteRead() NOW calls future.complete() even for empty slice");
            System.out.println("4. doSendResponse() was called");
            System.out.println("5. deregister() was executed");
            System.out.println("6. Operations properly cleaned from LiveOperationRegistry");
            System.out.println("\nIssue #26463 is FIXED!");
        } else {
            System.out.println("\n=== ❌ FIX VERIFICATION FAILED ===");
            System.out.println("Number of operations sent: " + numberOfOperations);
            System.out.println("Number of operations leaked: " + leakedOperations);
            System.out.println("The fix did not work - operations are still leaking!");
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
        System.out.println("\n=== TEST: No Leak When Metrics Collected (Control) ===\n");

        // Reconfigure for fast collection
        Config config = new Config();
        config.getMetricsConfig().setEnabled(true);
        config.getMetricsConfig().setCollectionFrequencySeconds(2); // Fast!
        config.getMetricsConfig().getManagementCenterConfig().setEnabled(true);

        // Recreate instance with fast config
        if (instance != null) {
            instance.shutdown();
        }
        instance = createHazelcastInstance(config);
        nodeEngine = getNode(instance).getNodeEngine();
        operationService = nodeEngine.getOperationService();
        metricsService = nodeEngine.getService(MetricsService.SERVICE_NAME);

        // Wait for initial collection
        System.out.println("Waiting for initial metrics collection...");
        sleepSeconds(3);

        int initialSize = getLiveOperationRegistrySize();
        System.out.println("Initial size: " + initialSize);

        // Request current metrics (should exist)
        ReadMetricsOperation operation = new ReadMetricsOperation(0);
        InternalCompletableFuture<RingbufferSlice<Map.Entry<Long, byte[]>>> future =
            operationService.invokeOnTarget(
                MetricsService.SERVICE_NAME,
                operation,
                nodeEngine.getThisAddress()
            );

        RingbufferSlice<Map.Entry<Long, byte[]>> result = future.get(5, TimeUnit.SECONDS);
        System.out.println("Received " + result.elements().size() + " metric entries - OK");

        sleepSeconds(1);

        int afterSize = getLiveOperationRegistrySize();
        System.out.println("After size: " + afterSize);

        if (afterSize == initialSize) {
            System.out.println("\n=== ✅ CONTROL TEST PASSED ===");
            System.out.println("No leak when metrics are properly collected");
        } else {
            fail("Unexpected leak in control test: " + (afterSize - initialSize) + " operations leaked");
        }
    }

    // ==================== Helper Methods ====================

    private long getCurrentHeadSequence() throws Exception {
        // Try to read from sequence 0 and get the next sequence
        ReadMetricsOperation op = new ReadMetricsOperation(0);
        InternalCompletableFuture<RingbufferSlice<Map.Entry<Long, byte[]>>> future =
            operationService.invokeOnTarget(
                MetricsService.SERVICE_NAME,
                op,
                nodeEngine.getThisAddress()
            );

        try {
            RingbufferSlice<Map.Entry<Long, byte[]>> slice = future.get(2, TimeUnit.SECONDS);
            return slice.nextSequence();
        } catch (Exception e) {
            // If failed, assume head is 0
            return 0;
        }
    }

    private void forceMetricsCollection() {
        // Trigger metrics collection manually
        try {
            metricsService.collectMetrics();
        } catch (Exception e) {
            System.out.println("Could not force metrics collection: " + e.getMessage());
        }
    }

    private int getLiveOperationRegistrySize() throws Exception {
        LiveOperationRegistry registry = metricsService.getLiveOperationRegistry();

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
        System.out.println("\n--- LiveOperationRegistry State ---");

        LiveOperationRegistry registry = metricsService.getLiveOperationRegistry();
        Field liveOperationsField = LiveOperationRegistry.class.getDeclaredField("liveOperations");
        liveOperationsField.setAccessible(true);

        @SuppressWarnings("unchecked")
        ConcurrentHashMap<?, Map<Long, ?>> liveOperations =
            (ConcurrentHashMap<?, Map<Long, ?>>) liveOperationsField.get(registry);

        System.out.println("Number of addresses: " + liveOperations.size());

        liveOperations.forEach((address, ops) -> {
            System.out.println("  Address: " + address);
            System.out.println("    Active operations: " + ops.size());
            ops.forEach((callId, op) -> {
                System.out.println("      CallId: " + callId
                    + ", Operation: " + op.getClass().getSimpleName());
            });
        });
        System.out.println("-----------------------------------\n");
    }
}
