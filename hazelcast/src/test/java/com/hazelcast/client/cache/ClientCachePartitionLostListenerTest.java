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

package com.hazelcast.client.cache;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.event.CachePartitionLostEvent;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.PartitionLostEventImpl;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.cache.CacheTestSupport.createClientCachingProvider;
import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.cache.impl.ICacheService.SERVICE_NAME;
import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCachePartitionLostListenerTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void test_cachePartitionLostListener_registered() {

        final String cacheName = randomName();

        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        final CachingProvider cachingProvider = createClientCachingProvider(client);
        final CacheManager cacheManager = cachingProvider.getCacheManager();
        final CacheConfig<Integer, String> cacheConfig = new CacheConfig<Integer, String>();
        final Cache<Integer, String> cache = cacheManager.createCache(cacheName, cacheConfig);
        final ICache iCache = cache.unwrap(ICache.class);

        iCache.addPartitionLostListener(new CachePartitionLostListener() {
            @Override
            public void partitionLost(CachePartitionLostEvent event) {
            }
        });

        assertRegistrationsSizeEventually(instance, cacheName, 1);
    }

    @Test
    public void test_cachePartitionLostListener_invoked() {
        final String cacheName = randomName();
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        final HazelcastServerCachingProvider cachingProvider = createServerCachingProvider(instance);
        final CacheManager cacheManager = cachingProvider.getCacheManager();
        final CacheConfig<Integer, String> config = new CacheConfig<Integer, String>();
        config.setBackupCount(0);
        cacheManager.createCache(cacheName, config);

        final CachingProvider clientCachingProvider = createClientCachingProvider(client);
        final CacheManager clientCacheManager = clientCachingProvider.getCacheManager();
        final Cache<Integer, String> cache = clientCacheManager.getCache(cacheName);

        final ICache iCache = cache.unwrap(ICache.class);

        final EventCollectingCachePartitionLostListener listener = new EventCollectingCachePartitionLostListener();
        iCache.addPartitionLostListener(listener);

        final CacheService cacheService = getNode(instance).getNodeEngine().getService(CacheService.SERVICE_NAME);
        final int partitionId = 5;
        cacheService.onPartitionLost(new PartitionLostEventImpl(partitionId, 0, null));

        assertCachePartitionLostEventEventually(listener, partitionId);
    }

    @Test
    public void test_cachePartitionLostListener_invoked_fromOtherNode() {

        final String cacheName = randomName();
        HazelcastInstance instance1 = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        final HazelcastServerCachingProvider cachingProvider = createServerCachingProvider(instance1);
        final CacheManager cacheManager = cachingProvider.getCacheManager();
        final CacheConfig<Integer, String> config = new CacheConfig<Integer, String>();
        config.setBackupCount(0);
        cacheManager.createCache(cacheName, config);

        final CachingProvider clientCachingProvider = createClientCachingProvider(client);
        final CacheManager clientCacheManager = clientCachingProvider.getCacheManager();
        final Cache<Integer, String> cache = clientCacheManager.getCache(cacheName);

        final ICache iCache = cache.unwrap(ICache.class);

        final EventCollectingCachePartitionLostListener listener = new EventCollectingCachePartitionLostListener();
        iCache.addPartitionLostListener(listener);

        assertRegistrationsSizeEventually(instance1, cacheName, 1);
        assertRegistrationsSizeEventually(instance2, cacheName, 1);

        final CacheService cacheService1 = getNode(instance1).getNodeEngine().getService(CacheService.SERVICE_NAME);
        final CacheService cacheService2 = getNode(instance2).getNodeEngine().getService(CacheService.SERVICE_NAME);
        final int partitionId = 5;
        cacheService1.onPartitionLost(new PartitionLostEventImpl(partitionId, 0, null));
        cacheService2.onPartitionLost(new PartitionLostEventImpl(partitionId, 0, null));

        assertCachePartitionLostEventEventually(listener, partitionId);
    }

    @Test
    public void test_cachePartitionLostListener_removed() {
        final String cacheName = randomName();
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        final HazelcastServerCachingProvider cachingProvider = createServerCachingProvider(instance);
        final CacheManager cacheManager = cachingProvider.getCacheManager();
        final CacheConfig<Integer, String> config = new CacheConfig<Integer, String>();
        config.setBackupCount(0);
        cacheManager.createCache(cacheName, config);

        final CachingProvider clientCachingProvider = createClientCachingProvider(client);
        final CacheManager clientCacheManager = clientCachingProvider.getCacheManager();
        final Cache<Integer, String> cache = clientCacheManager.getCache(cacheName);
        final ICache iCache = cache.unwrap(ICache.class);

        final UUID registrationId = iCache.addPartitionLostListener(mock(CachePartitionLostListener.class));

        assertRegistrationsSizeEventually(instance, cacheName, 1);

        assertTrue(iCache.removePartitionLostListener(registrationId));
        assertRegistrationsSizeEventually(instance, cacheName, 0);
    }

    private void assertCachePartitionLostEventEventually(final EventCollectingCachePartitionLostListener listener,
                                                         final int partitionId) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {

                final List<CachePartitionLostEvent> events = listener.getEvents();
                assertFalse(events.isEmpty());
                assertEquals(partitionId, events.get(0).getPartitionId());

            }
        });
    }

    private void assertRegistrationsSizeEventually(final HazelcastInstance instance, final String cacheName, final int size) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final EventService eventService = getNode(instance).getNodeEngine().getEventService();
                final Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, cacheName);
                assertEquals(size, registrations.size());
            }
        });
    }

    private static class EventCollectingCachePartitionLostListener
            implements CachePartitionLostListener {

        private final List<CachePartitionLostEvent> events
                = Collections.synchronizedList(new LinkedList<CachePartitionLostEvent>());

        EventCollectingCachePartitionLostListener() {
        }

        @Override
        public void partitionLost(CachePartitionLostEvent event) {
            this.events.add(event);
        }

        public List<CachePartitionLostEvent> getEvents() {
            synchronized (events) {
                return new ArrayList<CachePartitionLostEvent>(events);
            }
        }
    }
}
