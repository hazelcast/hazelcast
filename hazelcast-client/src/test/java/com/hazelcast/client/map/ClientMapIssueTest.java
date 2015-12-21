/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.map;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientProperty;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapIssueTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testListenerRegistrations() throws Exception {
        Config config = getConfig();
        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        final String mapName = randomMapName();
        IMap<Object, Object> map = client.getMap(mapName);
        map.addEntryListener(new EntryAdapter<Object, Object>(), true);

        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance(config);

        instance1.getLifecycleService().terminate();
        instance1 = hazelcastFactory.newHazelcastInstance(config);

        final EventService eventService1 = TestUtil.getNode(instance1).nodeEngine.getEventService();
        final EventService eventService2 = TestUtil.getNode(instance2).nodeEngine.getEventService();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Collection<EventRegistration> regs1 = eventService1.getRegistrations(MapService.SERVICE_NAME, mapName);
                Collection<EventRegistration> regs2 = eventService2.getRegistrations(MapService.SERVICE_NAME, mapName);

                assertEquals("there should be only one registration", 1, regs1.size());
                assertEquals("there should be only one registration", 1, regs2.size());
            }
        });
    }

    @Test
    @Category(NightlyTest.class)
    public void testOperationNotBlockingAfterClusterShutdown() throws InterruptedException {
        Config config = getConfig();
        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setExecutorPoolSize(1);
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS, "10");

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        final IMap<String, String> map = client.getMap(randomMapName());

        map.put(randomString(), randomString());

        instance1.getLifecycleService().terminate();
        instance2.getLifecycleService().terminate();

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    map.get(randomString());
                } catch (Exception ignored) {
                } finally {
                    latch.countDown();
                }
            }
        }.start();

        assertOpenEventually(latch);
    }

    @Test
    @Category(NightlyTest.class)
    public void testOperationNotBlockingAfterClusterShutdown_withOneExecutorPoolSize() throws InterruptedException {
        Config config = getConfig();
        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setExecutorPoolSize(1);
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS, "10");

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        final IMap<String, String> map = client.getMap(randomMapName());

        map.put(randomString(), randomString());

        instance1.getLifecycleService().terminate();
        instance2.getLifecycleService().terminate();

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    map.get(randomString());
                } catch (Exception ignored) {
                } finally {
                    latch.countDown();
                }
            }
        }.start();

        assertOpenEventually(latch);
    }

    @Test
    public void testMapPagingEntries() {
        Config config = getConfig();
        hazelcastFactory.newHazelcastInstance(config);
        hazelcastFactory.newHazelcastInstance(config);

        final ClientConfig clientConfig = new ClientConfig();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        final IMap<Integer, Integer> map = client.getMap("map");

        final int size = 50;
        final int pageSize = 5;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        final PagingPredicate predicate = new PagingPredicate(pageSize);
        predicate.nextPage();

        final Set<Map.Entry<Integer, Integer>> entries = map.entrySet(predicate);
        assertEquals(pageSize, entries.size());
    }

    @Test
    public void testMapPagingValues() {
        Config config = getConfig();
        hazelcastFactory.newHazelcastInstance(config);
        hazelcastFactory.newHazelcastInstance(config);

        final ClientConfig clientConfig = new ClientConfig();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        final IMap<Integer, Integer> map = client.getMap("map");

        final int size = 50;
        final int pageSize = 5;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        final PagingPredicate predicate = new PagingPredicate(pageSize);
        predicate.nextPage();

        final Collection<Integer> values = map.values(predicate);
        assertEquals(pageSize, values.size());
    }

    @Test
    public void testMapPagingKeySet() {
        Config config = getConfig();
        hazelcastFactory.newHazelcastInstance(config);
        hazelcastFactory.newHazelcastInstance(config);

        final ClientConfig clientConfig = new ClientConfig();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        final IMap<Integer, Integer> map = client.getMap("map");

        final int size = 50;
        final int pageSize = 5;
        for (int i = 0; i < size; i++) {
            map.put(size - i, i);
        }

        final PagingPredicate predicate = new PagingPredicate(pageSize);
        predicate.nextPage();

        final Set<Integer> values = map.keySet(predicate);
        assertEquals(pageSize, values.size());
    }


    @Test
    public void testNoOperationTimeoutException_whenUserCodeLongRunning() {
        Config config = getConfig();
        config.setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "100");
        hazelcastFactory.newHazelcastInstance(config);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        IMap<Object, Object> map = client.getMap(randomMapName());
        SleepyProcessor sleepyProcessor = new SleepyProcessor(2000);
        String key = randomString();
        String value = randomString();
        map.put(key, value);
        assertEquals(value, map.executeOnKey(key, sleepyProcessor));
    }

    static class SleepyProcessor implements EntryProcessor, Serializable {

        private long millis;

        SleepyProcessor(long millis) {
            this.millis = millis;
        }

        public Object process(Map.Entry entry) {
            try {

                Thread.sleep(millis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return entry.getValue();
        }

        public EntryBackupProcessor getBackupProcessor() {
            return null;
        }

    }
}
