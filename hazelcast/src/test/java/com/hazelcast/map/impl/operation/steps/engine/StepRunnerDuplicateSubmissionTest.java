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

package com.hazelcast.map.impl.operation.steps.engine;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.map.impl.operation.SetOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Reproduces duplicate execution of a step-based operation: when the
 * same operation instance is re-submitted (as a local invocation
 * retry does) while its first execution is parked in an offloaded
 * MapStore step, the step machinery starts a second, concurrent
 * execution of the very same operation.
 * <p>
 * For NATIVE maps this concurrent double-execution leads to
 * use-after-free of record memory and crashes the JVM.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StepRunnerDuplicateSubmissionTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "stepRunnerDuplicateSubmissionTest";
    private static final int PARTITION_ID = 1;

    @Test
    public void resubmitted_operation_is_not_executed_twice() throws Exception {
        BlockingMapStore mapStore = new BlockingMapStore();

        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.getMapConfig(MAP_NAME)
                .setMapStoreConfig(new MapStoreConfig()
                        .setEnabled(true)
                        .setOffload(true)
                        .setImplementation(mapStore));

        HazelcastInstance node = createHazelcastInstance(config);

        // Ensure initial MapLoader loading has finished, otherwise
        // operations are rejected with a retryable "map is still
        // loading" exception before reaching their store step.
        node.getMap(MAP_NAME).size();

        Data key = Accessors.getSerializationService(node).toData("key");
        Data value = Accessors.getSerializationService(node).toData("value");

        Operation operation = new SetOperation(MAP_NAME, key, value)
                .setPartitionId(PARTITION_ID)
                .setNodeEngine(Accessors.getNodeEngineImpl(node));

        AtomicInteger responseCount = new AtomicInteger();
        AtomicReference<Object> lastResponse = new AtomicReference<>();
        operation.setOperationResponseHandler((Operation op, Object response) -> {
            responseCount.incrementAndGet();
            lastResponse.set(response);
        });

        // 1st submission: operation starts and parks inside the
        // blocking MapStore.store() on the offloaded executor.
        Accessors.getOperationService(node).execute(operation);
        boolean reachedStore = mapStore.storeEntered.await(10, TimeUnit.SECONDS);
        assertTrue("operation did not reach MapStore.store()"
                + " [loadCount=" + mapStore.loadCount.get()
                + ", storeCount=" + mapStore.storeCount.get()
                + ", responseCount=" + responseCount.get()
                + ", lastResponse=" + lastResponse.get() + "]", reachedStore);

        // 2nd submission of the SAME instance, while the 1st execution
        // is still parked.
        Accessors.getOperationService(node).execute(operation);

        // Give the duplicate execution time to reach the
        // MapStore as well, then release all parked store() calls.
        sleepSeconds(2);
        mapStore.proceed.countDown();

        assertTrueEventually(() -> {
            assertTrue("no store() completed", mapStore.storeCount.get() >= 1);
            assertTrue("operation did not send a response", responseCount.get() >= 1);
        });

        // Allow a potential duplicate execution to finish, while verifying that
        // neither execution nor response handling happens more than once.
        assertTrueAllTheTime(() -> {
            assertEquals("same operation instance was executed more than once",
                    1, mapStore.storeCount.get());
            assertEquals("same operation instance sent more than one response",
                    1, responseCount.get());
        }, 3);
    }

    private static final class BlockingMapStore extends MapStoreAdapter<Object, Object> {

        final CountDownLatch storeEntered = new CountDownLatch(1);
        final CountDownLatch proceed = new CountDownLatch(1);
        final AtomicInteger storeCount = new AtomicInteger();
        final AtomicInteger loadCount = new AtomicInteger();

        @Override
        public void store(Object key, Object value) {
            storeEntered.countDown();
            try {
                proceed.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            storeCount.incrementAndGet();
        }

        @Override
        public Object load(Object key) {
            loadCount.incrementAndGet();
            return null;
        }
    }
}
