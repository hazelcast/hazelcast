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

package com.hazelcast.jet.impl.util;

import com.hazelcast.config.Config;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.AsyncMapWriter.MAX_PARALLEL_ASYNC_OPS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class AsyncMapWriterTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;
    private JetTestInstanceFactory factory;
    private JetInstance instance1;
    private JetInstance instance2;
    private AsyncMapWriter writer;
    private IStreamMap<Object, Object> map;
    private NodeEngineImpl nodeEngine;


    @Before
    public void setup() {
        factory = new JetTestInstanceFactory();
        JetConfig jetConfig = new JetConfig();
        Config config = jetConfig.getHazelcastConfig();

        // use one partition per member
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(NODE_COUNT * 2));
        config.setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "3000");
        config.getMapConfig("default")
              .setBackupCount(1)
              .setAsyncBackupCount(0);

        JetInstance[] instances = factory.newMembers(jetConfig, NODE_COUNT);
        instance1 = instances[0];
        instance2 = instances[1];
        nodeEngine = getNodeEngineImpl(instance1.getHazelcastInstance());
        writer = new AsyncMapWriter(nodeEngine);
        map = instance1.getMap("testMap");
        writer.setMapName(map.getName());
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
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
        assertTrue("tryFlushAsync failed", flushed);

        future.get();

        assertEquals("value1", map.get("key1"));
        assertEquals("value2", map.get("key2"));
        assertEquals("value3", map.get("key3"));
    }

    @Test
    public void when_emptyFlush_then_futureIscompleted() throws Exception {
        // Given
        CompletableFuture<Void> future = new CompletableFuture<>();

        // When
        boolean flushed = writer.tryFlushAsync(future);

        // Then
        assertTrue("tryFlushAsync failed", flushed);
        future.get();
    }

    @Test
    public void when_flushedSeveralTimes_then_doesPut() throws Exception {
        // When
        List<CompletableFuture> futures = new ArrayList<>();
        for (int i = 0; i < MAX_PARALLEL_ASYNC_OPS; i++) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            writer.put(i, i);
            assertTrue("tryFlushAsync failed", writer.tryFlushAsync(future));
            futures.add(future);
        }

        // Then
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        for (int i = 0; i < MAX_PARALLEL_ASYNC_OPS; i++) {
            assertEquals(i, map.get(i));
        }
    }

    @Test
    public void when_tooManyConcurrentOps_then_refuseFlush() throws Exception {
        // Given
        writer.put("key1", "value1");
        writer.put("key2", "value2");
        writer.put("key3", "value3");
        writer.put("key4", "value4");
        CompletableFuture<Void> future = new CompletableFuture<>();

        JetService service = nodeEngine.getService(JetService.SERVICE_NAME);
        service.numConcurrentPutAllOps().set(MAX_PARALLEL_ASYNC_OPS - NODE_COUNT + 1);
        // When
        boolean flushed = writer.tryFlushAsync(future);

        // Then
        assertFalse("tryFlushAsync should fail", flushed);
    }

    @Test
    public void when_opFinished_then_releaseCount() throws Exception {
        // Given
        writer.put("key1", "value1");
        CompletableFuture<Void> future = new CompletableFuture<>();

        JetService service = nodeEngine.getService(JetService.SERVICE_NAME);
        service.numConcurrentPutAllOps().set(MAX_PARALLEL_ASYNC_OPS - NODE_COUNT);
        // When
        assertTrue("tryFlushAsync failed", writer.tryFlushAsync(future));

        future.get();

        writer.put("key2", "value2");
        boolean flushed = writer.tryFlushAsync(future);

        // Then
        assertTrue("tryFlushAsync failed", flushed);
    }

    @Test
    @Ignore //TODO: investigate intermittent failure
    public void when_memberLeaves_then_retryAutomatically() throws Exception {
        // Given
        for (int i = 0; i < 100; i++) {
            writer.put(i, i);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();

        JetService service = nodeEngine.getService(JetService.SERVICE_NAME);
        service.numConcurrentPutAllOps().set(MAX_PARALLEL_ASYNC_OPS - NODE_COUNT);

        // When
        assertTrue("tryFlushAsync failed", writer.tryFlushAsync(future));
        factory.terminate(instance2);

        // Then
        future.get();
        for (int i = 0; i < 100; i++) {
            assertEquals(i, map.get(i));
        }

    }
}
