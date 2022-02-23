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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheEventData;
import com.hazelcast.cache.impl.CacheEventDataImpl;
import com.hazelcast.cache.impl.CacheEventSet;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cluster.Address;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.collection.impl.collection.CollectionEvent;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.impl.queue.QueueEvent;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.internal.longregister.LongRegisterService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.ItemCounter;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.event.EntryEventData;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.eventservice.impl.LocalEventDispatcher;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.REMOVED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EventQueuePluginTest extends AbstractDiagnosticsPluginTest {

    private static final int EVENT_COUNTER = 1000;

    private static final String MAP_NAME = "myMap";
    private static final String CACHE_NAME = "myCache";
    private static final String QUEUE_NAME = "myQueue";
    private static final String LIST_NAME = "myList";
    private static final String SET_NAME = "mySet";

    private final CountDownLatch listenerLatch = new CountDownLatch(1);

    private HazelcastInstance hz;
    private EventQueuePlugin plugin;
    private ItemCounter<String> itemCounter;

    @Before
    public void setup() {
        Config config = new Config()
                .setProperty(EventQueuePlugin.PERIOD_SECONDS.getName(), "1")
                .setProperty(EventQueuePlugin.SAMPLES.getName(), "100")
                .setProperty(EventQueuePlugin.THRESHOLD.getName(), "10");

        hz = createHazelcastInstance(config);

        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        EventServiceImpl eventService = (EventServiceImpl) nodeEngine.getEventService();

        plugin = new EventQueuePlugin(nodeEngine, eventService.getEventExecutor());
        plugin.onStart();

        itemCounter = plugin.getOccurrenceMap();

        warmUpPartitions(hz);
    }

    @Test
    public void testMap() {
        final IMap<Integer, Integer> map = hz.getMap(MAP_NAME);
        map.addLocalEntryListener(new EntryAddedListener<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                assertOpenEventually(listenerLatch);
            }
        });
        map.addLocalEntryListener(new EntryRemovedListener() {
            @Override
            public void entryRemoved(EntryEvent event) {
                assertOpenEventually(listenerLatch);
            }
        });

        spawn(new Runnable() {
            @Override
            public void run() {
                Random random = new Random();
                for (int i = 0; i < EVENT_COUNTER; i++) {
                    int key = random.nextInt(Integer.MAX_VALUE);
                    map.putAsync(key, 23);
                    map.removeAsync(key);
                }
            }
        });

        assertContainsEventually(
                "IMap '" + MAP_NAME + "' ADDED sampleCount=",
                "IMap '" + MAP_NAME + "' REMOVED sampleCount=");
    }

    @Test
    public void testCache() {
        CompleteConfiguration<Integer, Integer> cacheConfig = new MutableConfiguration<Integer, Integer>()
                .addCacheEntryListenerConfiguration(new MutableCacheEntryListenerConfiguration<Integer, Integer>(
                        FactoryBuilder.factoryOf(new TestCacheListener()), null, true, true));

        CachingProvider memberProvider = createServerCachingProvider(hz);
        HazelcastServerCacheManager memberCacheManager = (HazelcastServerCacheManager) memberProvider.getCacheManager();
        final ICache<Integer, Integer> cache = memberCacheManager.createCache(CACHE_NAME, cacheConfig);

        spawn(new Runnable() {
            @Override
            public void run() {
                Random random = new Random();
                for (int i = 0; i < EVENT_COUNTER; i++) {
                    int key = random.nextInt(Integer.MAX_VALUE);
                    cache.putAsync(key, 23);
                    cache.removeAsync(key);
                }
            }
        });

        assertContainsEventually(
                "ICache '/hz/" + CACHE_NAME + "' CREATED sampleCount=",
                "ICache '/hz/" + CACHE_NAME + "' REMOVED sampleCount=");
    }

    @Test
    public void testQueue() {
        final IQueue<Integer> queue = hz.getQueue(QUEUE_NAME);
        queue.addItemListener(new TestItemListener(), true);

        spawn(new Runnable() {
            @Override
            public void run() {
                Random random = new Random();
                for (int i = 0; i < EVENT_COUNTER; i++) {
                    int key = random.nextInt(Integer.MAX_VALUE);
                    queue.add(key);
                    queue.poll();
                }
            }
        });

        assertContainsEventually(
                "IQueue '" + QUEUE_NAME + "' ADDED sampleCount=",
                "IQueue '" + QUEUE_NAME + "' REMOVED sampleCount=");
    }

    @Test
    public void testList() {
        final IList<Integer> list = hz.getList(LIST_NAME);
        list.addItemListener(new TestItemListener(), true);

        spawn(new Runnable() {
            @Override
            public void run() {
                Random random = new Random();
                for (int i = 0; i < EVENT_COUNTER; i++) {
                    int key = random.nextInt(EVENT_COUNTER);
                    list.add(key);
                }
            }
        });

        assertContainsEventually("IList '" + LIST_NAME + "' ADDED sampleCount=");
    }

    @Test
    public void testSet() {
        final ISet<Integer> set = hz.getSet(SET_NAME);
        set.addItemListener(new TestItemListener(), true);

        spawn(new Runnable() {
            @Override
            public void run() {
                Random random = new Random();
                for (int i = 0; i < EVENT_COUNTER; i++) {
                    int key = random.nextInt(Integer.MAX_VALUE);
                    set.add(key);
                    set.remove(key);
                }
            }
        });

        assertContainsEventually(
                "ISet '" + SET_NAME + "' ADDED sampleCount=",
                "ISet '" + SET_NAME + "' REMOVED sampleCount=");
    }

    @Test
    public void testSampleRunnable() {
        Address caller = new Address();
        Data data = mock(Data.class);

        EntryEventData mapEventAdded = new EntryEventData("source", "mapName", caller, data, data, data, ADDED.getType());
        EntryEventData mapEventUpdated = new EntryEventData("source", "mapName", caller, data, data, data, UPDATED.getType());
        EntryEventData mapEventRemoved = new EntryEventData("source", "mapName", caller, data, data, data, REMOVED.getType());
        assertSampleRunnable("IMap 'mapName' ADDED", mapEventAdded, MapService.SERVICE_NAME);
        assertSampleRunnable("IMap 'mapName' UPDATED", mapEventUpdated, MapService.SERVICE_NAME);
        assertSampleRunnable("IMap 'mapName' REMOVED", mapEventRemoved, MapService.SERVICE_NAME);

        CacheEventData cacheEventCreated = new CacheEventDataImpl("cacheName", CacheEventType.CREATED, data, data, data, true);
        CacheEventData cacheEventUpdated = new CacheEventDataImpl("cacheName", CacheEventType.UPDATED, data, data, data, true);
        CacheEventData cacheEventRemoved = new CacheEventDataImpl("cacheName", CacheEventType.REMOVED, data, data, data, true);
        CacheEventSet CacheEventSetCreated = new CacheEventSet(CacheEventType.CREATED, singleton(cacheEventCreated), 1);
        CacheEventSet CacheEventSetUpdated = new CacheEventSet(CacheEventType.UPDATED, singleton(cacheEventUpdated), 1);
        CacheEventSet cacheEventSetRemoved = new CacheEventSet(CacheEventType.REMOVED, singleton(cacheEventRemoved), 1);
        assertSampleRunnable("ICache 'cacheName' CREATED", CacheEventSetCreated, CacheService.SERVICE_NAME);
        assertSampleRunnable("ICache 'cacheName' UPDATED", CacheEventSetUpdated, CacheService.SERVICE_NAME);
        assertSampleRunnable("ICache 'cacheName' REMOVED", cacheEventSetRemoved, CacheService.SERVICE_NAME);

        List<CacheEventData> cacheEventData = asList(cacheEventCreated, cacheEventUpdated, cacheEventRemoved);
        Set<CacheEventData> cacheEvents = new HashSet<CacheEventData>(cacheEventData);
        CacheEventSet cacheEventSetAll = new CacheEventSet(CacheEventType.EXPIRED, cacheEvents, 1);
        assertCacheEventSet(cacheEventSetAll,
                "ICache 'cacheName' CREATED",
                "ICache 'cacheName' UPDATED",
                "ICache 'cacheName' REMOVED");

        QueueEvent queueEventAdded = new QueueEvent("queueName", data, ItemEventType.ADDED, caller);
        QueueEvent queueEventRemoved = new QueueEvent("queueName", data, ItemEventType.REMOVED, caller);
        assertSampleRunnable("IQueue 'queueName' ADDED", queueEventAdded, QueueService.SERVICE_NAME);
        assertSampleRunnable("IQueue 'queueName' REMOVED", queueEventRemoved, QueueService.SERVICE_NAME);

        CollectionEvent setEventAdded = new CollectionEvent("setName", data, ItemEventType.ADDED, caller);
        CollectionEvent setEventRemoved = new CollectionEvent("setName", data, ItemEventType.REMOVED, caller);
        assertSampleRunnable("ISet 'setName' ADDED", setEventAdded, SetService.SERVICE_NAME);
        assertSampleRunnable("ISet 'setName' REMOVED", setEventRemoved, SetService.SERVICE_NAME);

        CollectionEvent listEventAdded = new CollectionEvent("listName", data, ItemEventType.ADDED, caller);
        CollectionEvent listEventRemoved = new CollectionEvent("listName", data, ItemEventType.REMOVED, caller);
        assertSampleRunnable("IList 'listName' ADDED", listEventAdded, ListService.SERVICE_NAME);
        assertSampleRunnable("IList 'listName' REMOVED", listEventRemoved, ListService.SERVICE_NAME);

        assertSampleRunnable("Object", new Object(), LongRegisterService.SERVICE_NAME);

        assertSampleRunnable(new TestEvent(), TestEvent.class.getName());
    }

    private void assertContainsEventually(final String... messages) {
        try {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    plugin.run(logWriter);

                    //System.out.println(getContent());

                    for (String message : messages) {
                        assertContains(message);
                    }
                }
            });
        } finally {
            listenerLatch.countDown();
        }
    }

    private void assertCacheEventSet(CacheEventSet cacheEventSet, String... expectedKeys) {
        LocalEventDispatcher e = createLocalEventDispatcher(cacheEventSet, CacheService.SERVICE_NAME);
        assertSampleRunnable(e, expectedKeys);
    }

    private void assertSampleRunnable(String expectedKey, Object event, String serviceName) {
        assertSampleRunnable(createLocalEventDispatcher(event, serviceName), expectedKey);
    }

    private void assertSampleRunnable(Runnable runnable, String... expectedKeys) {
        assertEqualsStringFormat("Expected to sample %d keys, but got %d", expectedKeys.length, plugin.sampleRunnable(runnable));
        Set<String> actualKeys = itemCounter.keySet();
        for (String expectedKey : expectedKeys) {
            assertTrue("Expected to find key [" + expectedKey + "] in " + actualKeys, actualKeys.contains(expectedKey));
        }

        itemCounter.clear();
    }

    private LocalEventDispatcher createLocalEventDispatcher(Object event, String serviceName) {
        EventServiceImpl eventService = mock(EventServiceImpl.class);
        return new LocalEventDispatcher(eventService, serviceName, event, null, 1, 1);
    }

    private final class TestCacheListener
            implements CacheEntryCreatedListener<Integer, Integer>, CacheEntryRemovedListener<Integer, Integer>, Serializable {

        TestCacheListener() {
        }

        @Override
        public void onCreated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> cacheEntryEvents)
                throws CacheEntryListenerException {
            assertOpenEventually(listenerLatch);
        }

        @Override
        public void onRemoved(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> cacheEntryEvents)
                throws CacheEntryListenerException {
            assertOpenEventually(listenerLatch);
        }
    }

    private final class TestItemListener implements ItemListener<Integer> {

        @Override
        public void itemAdded(ItemEvent<Integer> item) {
            assertOpenEventually(listenerLatch);
        }

        @Override
        public void itemRemoved(ItemEvent<Integer> item) {
            assertOpenEventually(listenerLatch);
        }
    }

    private static class TestEvent implements Runnable {

        @Override
        public void run() {
        }
    }
}
