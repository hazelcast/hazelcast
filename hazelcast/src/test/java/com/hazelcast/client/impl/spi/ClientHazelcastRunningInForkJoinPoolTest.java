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

package com.hazelcast.client.impl.spi;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientHazelcastRunningInForkJoinPoolTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance client;
    private HazelcastInstance server;
    private IMap serverMap;
    private IMap clientMap;
    private String slowLoadingMapName = "slowLoadingMap";
    private String defaultMapName = "default";

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new MapStoreAdapter() {
            @Override
            public Object load(Object key) {
                sleepSeconds(1000);
                return super.load(key);
            }
        });

        MapConfig mapConfig = new MapConfig(slowLoadingMapName);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        Config config = smallInstanceConfigWithoutJetAndMetrics().addMapConfig(mapConfig);

        server = hazelcastFactory.newHazelcastInstance(config);
        client = hazelcastFactory.newHazelcastClient();

        serverMap = server.getMap(defaultMapName);
        clientMap = client.getMap(slowLoadingMapName);
    }

    @Test
    public void slow_data_loading_does_not_block_entry_listener_addition() throws ExecutionException, InterruptedException {
        CountDownLatch tasksSubmitted = new CountDownLatch(1);
        ForkJoinPool commonPool = ForkJoinPool.commonPool();

        // 1. Let's simulate some parallel tasks
        // contend on loading 1 item from a database.
        int count = 2 * Runtime.getRuntime().availableProcessors();
        for (int i = 0; i < count; i++) {
            commonPool.submit(() -> {
                // Before calling map#get and expand the pool
                // we need to wait for all tasks to be submitted
                // because if there is a delay between running
                // task-X and submission of task-X+1, in Java-8
                // the FJP could try to terminate the spare thread
                // created while running task-X, thinking that the
                // pool is now quiescent. It might cause task-X+1
                // and any further submissions to FJP not run because
                // there is now no spare threads.
                assertOpenEventually(tasksSubmitted);
                clientMap.get(1);
            });
        }

        // Wait until all the initial FJP threads got a map#get task,
        // and the rest of the tasks are waiting in the queue, so that
        // the assertions below make sense.
        assertTrueEventually(() -> assertEquals(count - commonPool.getParallelism(), commonPool.getQueuedSubmissionCount()));

        Future addListenerFuture = spawn(() -> {
            // 2. In parallel, adding a listener to a different
            // map must not be affected by loading phase at step 1.
            serverMap.addEntryListener(new EntryAdapter<>(), true);
        });

        // The task for add listener call above would eventually be
        // queued and that would guarantee that the spare FJP thread
        // created for the last map#get call above wouldn't terminate
        // before completing it.
        assertTrueEventually(() -> assertTrue(commonPool.getQueuedSubmissionCount() > count - commonPool.getParallelism()));

        // All tasks are submitted, if the ManagedBlocker is implemented
        // properly for the client futures, the FJP would expand and run
        // the task for listener addition as well.
        tasksSubmitted.countDown();

        addListenerFuture.get();
    }
}
