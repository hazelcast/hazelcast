/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.util;

import com.hazelcast.client.map.helpers.AMapStore;
import com.hazelcast.config.Config;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
public class AsyncMapWriterTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;
    private static final String ALWAYS_FAILING_MAP = "alwaysFailingMap";
    private static final String RETRYABLE_MAP = "retryableMap";

    private JetInstance instance1;
    private JetInstance instance2;
    private AsyncMapWriter writer;
    private IMapJet<Object, Object> map;
    private NodeEngineImpl nodeEngine;

    @Before
    public void setup() {
        JetConfig jetConfig = new JetConfig();
        Config config = jetConfig.getHazelcastConfig();

        // use two partitions per member
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(NODE_COUNT * 2));
        config.setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "3000");
        config.getMapConfig("default")
              .setBackupCount(1)
              .setAsyncBackupCount(0);

        config.getMapConfig(ALWAYS_FAILING_MAP)
              .getMapStoreConfig()
              .setEnabled(true)
              .setImplementation(new AlwaysFailingMapStore());

        config.getMapConfig(RETRYABLE_MAP)
              .getMapStoreConfig()
              .setEnabled(true)
              .setImplementation(new RetryableMapStore());

        JetInstance[] instances = createJetMembers(jetConfig, NODE_COUNT);
        instance1 = instances[0];
        instance2 = instances[1];
        nodeEngine = getNodeEngineImpl(instance1.getHazelcastInstance());
        writer = new AsyncMapWriter(nodeEngine);
        map = instance1.getMap("testMap");
        writer.setMapName(map.getName());
    }

    @Test
    public void when_flush_then_doesPut() throws Exception {
        // Given
        writer.put("key1", "value1");
        writer.put("key2", "value2");
        writer.put("key3", "value3");
        CompletableFuture<Void> future = new CompletableFuture<>();

        // When
        boolean flushed = writer.tryFlushAsync(future);

        // Then
        assertTrue("tryFlushAsync returned false", flushed);

        future.get();

        assertEquals("value1", map.get("key1"));
        assertEquals("value2", map.get("key2"));
        assertEquals("value3", map.get("key3"));
        assertEquals(3, map.size());
    }

    @Test
    public void when_emptyFlush_then_futureIsCompleted() throws Exception {
        // Given
        CompletableFuture<Void> future = new CompletableFuture<>();

        // When
        boolean flushed = writer.tryFlushAsync(future);

        // Then
        assertTrue("tryFlushAsync returned false", flushed);
        future.get();
    }

    @Test
    public void when_flushedSeveralTimes_then_doesPut() throws Exception {
        // When
        List<CompletableFuture> futures = new ArrayList<>();
        for (int i = 0; i < JetService.MAX_PARALLEL_ASYNC_OPS; i++) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            writer.put(i, i);
            assertTrue("tryFlushAsync returned false", writer.tryFlushAsync(future));
            futures.add(future);
        }

        // Then
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        for (int i = 0; i < JetService.MAX_PARALLEL_ASYNC_OPS; i++) {
            assertEquals(i, map.get(i));
        }
    }

    @Test
    public void when_tooManyConcurrentOps_then_refuseFlush() {
        // Given
        writer.put("key1", "value1");
        writer.put("key2", "value2");
        writer.put("key3", "value3");
        writer.put("key4", "value4");
        CompletableFuture<Void> future = new CompletableFuture<>();

        JetService service = nodeEngine.getService(JetService.SERVICE_NAME);
        service.numConcurrentAsyncOps().set(JetService.MAX_PARALLEL_ASYNC_OPS - NODE_COUNT + 1);
        // When
        boolean flushed = writer.tryFlushAsync(future);

        // Then
        assertFalse("tryFlushAsync should return false", flushed);
    }

    @Test
    public void when_opFinished_then_releaseCount() throws Exception {
        // Given
        writer.put("key1", "value1");
        CompletableFuture<Void> future = new CompletableFuture<>();

        JetService service = nodeEngine.getService(JetService.SERVICE_NAME);
        service.numConcurrentAsyncOps().set(JetService.MAX_PARALLEL_ASYNC_OPS - NODE_COUNT);
        // When
        boolean flushed = writer.tryFlushAsync(future);
        assertTrue("tryFlushAsync returned false", flushed);

        future.get();

        writer.put("key2", "value2");
        flushed = writer.tryFlushAsync(future);

        // Then
        assertTrue("tryFlushAsync returned false", flushed);
    }

    @Test
    @Ignore("This test is currently racy as an operation is only retried once.")
    public void when_memberLeaves_then_retryAutomatically() throws Exception {
        // Given
        for (int i = 0; i < 100; i++) {
            writer.put(i, i);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();

        JetService service = nodeEngine.getService(JetService.SERVICE_NAME);
        service.numConcurrentAsyncOps().set(JetService.MAX_PARALLEL_ASYNC_OPS - NODE_COUNT);

        // When
        boolean flushed = writer.tryFlushAsync(future);
        assertTrue("tryFlushAsync returned false", flushed);
        terminateInstance(instance2);

        // Then
        future.get();
        for (int i = 0; i < 100; i++) {
            assertEquals(i, map.get(i));
        }
    }

    @Test
    public void when_writeError_then_failFuture() {
        // Given

        // make sure map store is initialized
        IMapJet<Object, Object> map = instance1.getMap(ALWAYS_FAILING_MAP);
        try {
            map.put(1, 1);
            fail("map.put() did not fail.");
        } catch (RuntimeException ignored) {
        }
        map.clear();

        writer.setMapName(ALWAYS_FAILING_MAP);
        for (int i = 0; i < 100; i++) {
            writer.put(i, i);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();

        // When
        boolean flushed = writer.tryFlushAsync(future);

        // Then
        assertTrue("tryFlushAsync() returned false", flushed);

        Throwable actual = future.handle((r, e) -> e).join();
        assertNotNull("No exception was thrown", actual);
        assertEquals("Always failing store", actual.getMessage());
    }

    @Test
    public void when_writeErrorOnlyOnce_then_retrySuccess() {
        // Given

        // make sure map store is initialized
        IMapJet<Object, Object> map = instance1.getMap(RETRYABLE_MAP);
        map.put(1, 1);
        map.clear();

        writer.setMapName(RETRYABLE_MAP);
        for (int i = 0; i < 100; i++) {
            writer.put(i, i);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();

        // When
        RetryableMapStore.failOnNext = true;
        boolean flushed = writer.tryFlushAsync(future);

        // Then
        assertTrue("tryFlushAsync() returned false", flushed);
        future.join();
        for (int i = 0; i < 100; i++) {
            assertEquals(i, map.get(i));
        }
    }

    static class AlwaysFailingMapStore extends AMapStore implements Serializable {

        @Override
        public void store(Object o, Object o2) {
            throw new RuntimeException("Always failing store");
        }
    }

    private static class RetryableMapStore extends AMapStore implements Serializable {

        private static volatile boolean failOnNext;

        @Override
        public void store(Object o, Object o2) {
            if (failOnNext) {
                failOnNext = false;
                throw new RetryableHazelcastException("Failing once store");
            }
        }
    }
}
