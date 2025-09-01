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
package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.impl.invalidation.BatchNearCacheInvalidation;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidation;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nearcache.NearCacheStats;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.hazelcast.spi.properties.ClusterProperty.EVENT_THREAD_COUNT;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@QuickTest
@ParallelJVMTest
class InvalidationEventBalancingTest
        extends HazelcastTestSupport {

    private final String testMapName = "testMap";
    private final int eventThreadCount = 5;
    private final int invalidationBatchSize = 5;

    private final int cacheLowerKey = 100;
    private final int cacheUpperKey = 600;

    private TestHazelcastFactory hazelcastFactory;

    @BeforeEach
    void setup() {
        hazelcastFactory = new TestHazelcastFactory();
    }

    @AfterEach
    void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    void testInvalidationDistribution() {
        HazelcastInstance hz = hazelcastFactory.newHazelcastInstance(createMemberConfig());
        IMap<Long, String> testMap = hz.getMap(testMapName);

        int targetBatchInvalidationCount = 500;
        int totalInvalidationsPerListener = Math.multiplyExact(targetBatchInvalidationCount, invalidationBatchSize);
        int totalInvalidations = Math.multiplyExact(totalInvalidationsPerListener, eventThreadCount);

        CountDownLatch invalidationWaitLatch = new CountDownLatch(totalInvalidations);
        List<InvalidationEventThreadTracker> listeners = IntStream.range(0, eventThreadCount).mapToObj(
                i -> new InvalidationEventThreadTracker(invalidationWaitLatch)).toList();

        registerListenerPerEventThread(getNodeEngineImpl(hz).getEventService(), listeners);

        for (long k = 0; k < totalInvalidationsPerListener; k++) {
            // Invalidation happens as part of the operation after the response returned, so they can
            // happen concurrently even with sequential synchronous puts like this.
            testMap.put(k, "" + k);
        }

        waitForInvalidations(invalidationWaitLatch);

        SoftAssertions assertionBundle = new SoftAssertions();

        Set<Thread> eventThreads = new HashSet<>();
        for (int i = 0; i < listeners.size(); i++) {
            InvalidationEventThreadTracker listener = listeners.get(i);
            String tag = "listener" + i;
            assertionBundle.assertThat(listener.invalidationCount.get()).as("Count for " + tag)
                           .isEqualTo(totalInvalidationsPerListener);
            eventThreads.addAll(listener.invalidationsPerThread.keySet());
        }
        assertionBundle.assertThat(eventThreads.size()).isEqualTo(eventThreadCount);

        assertionBundle.assertAll();
    }

    private Config createMemberConfig() {
        return smallInstanceConfig().setProperty(EVENT_THREAD_COUNT.getName(), "" + eventThreadCount)
                                    .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), "" + invalidationBatchSize)
                                    .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), "5");
    }

    private void waitForInvalidations(CountDownLatch waitLatch) {
        try {
            if (!waitLatch.await(30, TimeUnit.SECONDS)) {
                fail("Invalidations were not processed");
            }
        } catch (InterruptedException e) {
            fail("Test was interrupted");
        }
    }

    private void registerListenerPerEventThread(EventService eventService, List<InvalidationEventThreadTracker> adapters) {
        assertThat(adapters.size()).isEqualTo(eventThreadCount);
        UUID[] registrations = new UUID[eventThreadCount];
        var adaptersCopy = new ArrayList<>(adapters);
        while (Arrays.stream(registrations).anyMatch(Objects::isNull)) {
            ListenerAdapter<Object> adapter = adaptersCopy.get(0);
            UUID registration = eventService.registerListener(MapService.SERVICE_NAME, testMapName,
                    new EventListenerFilter(EntryEventType.INVALIDATION.getType(), TrueEventFilter.INSTANCE), adapter).getId();

            int queue = HashUtil.hashToIndex(registration.hashCode(), eventThreadCount);
            if (registrations[queue] == null) {
                registrations[queue] = registration;
                adaptersCopy.remove(0);
            } else {
                eventService.deregisterListener(MapService.SERVICE_NAME, testMapName, registration);
            }
        }
    }

    private static class InvalidationEventThreadTracker
            implements ListenerAdapter<Object> {

        final CountDownLatch invalidationWaitLatch;
        final ConcurrentMap<Thread, Integer> invalidationsPerThread = new ConcurrentHashMap<>();
        final AtomicInteger invalidationCount = new AtomicInteger();

        InvalidationEventThreadTracker(CountDownLatch invalidationWaitLatch) {
            this.invalidationWaitLatch = invalidationWaitLatch;
        }

        @Override
        public void onEvent(Object event) {
            if (event instanceof BatchNearCacheInvalidation batchInvalidation) {
                invalidationsPerThread.put(Thread.currentThread(), 1);
                for (Invalidation ignored : batchInvalidation.getInvalidations()) {
                    invalidationCount.incrementAndGet();
                    invalidationWaitLatch.countDown();
                }
            }
        }
    }

    @Test
    void testNearCacheInvalidations() {
        HazelcastInstance[] members = hazelcastFactory.newInstances(createMemberConfig(), 3);

        String testMapName = "testMap";
        int cacheCapacity = cacheUpperKey - cacheLowerKey;
        ClientConfig clientConfig = new ClientConfig().addNearCacheConfig(
                new NearCacheConfig().setName(testMapName).setEvictionConfig(new EvictionConfig().setSize(cacheCapacity)));

        HazelcastInstance nearCacheClient = hazelcastFactory.newHazelcastClient(clientConfig);

        Set<Member> expectedClusterMembers = Arrays.stream(members).map(hz -> hz.getCluster().getLocalMember()).collect(toSet());
        assertTrueEventually(() -> assertThat(nearCacheClient.getCluster().getMembers()).isEqualTo(expectedClusterMembers));

        IMap<Integer, Integer> clientTestMap = nearCacheClient.getMap(testMapName);
        NearCacheStats cacheStats = clientTestMap.getLocalMapStats().getNearCacheStats();

        putRange(clientTestMap, 0, 1000);

        assertTrueEventually(() -> assertThat(cacheStats.getInvalidationRequests()).isEqualTo(1000));
        assertTrueEventually(() -> assertThat(cacheStats.getInvalidations()).isEqualTo(0));
        assertTrueEventually(() -> assertThat(cacheStats.getOwnedEntryCount()).isEqualTo(0));

        getCacheRange(clientTestMap);

        assertTrueEventually(() -> assertThat(cacheStats.getOwnedEntryCount()).isEqualTo(cacheCapacity));
        assertTrueEventually(() -> assertThat(cacheStats.getMisses()).isEqualTo(cacheCapacity));
        assertTrueEventually(() -> assertThat(cacheStats.getHits()).isEqualTo(0));

        IMap<Integer, Integer> memberTestMap = members[0].getMap(testMapName);
        putRange(memberTestMap, cacheLowerKey, cacheLowerKey + 100);
        putRange(memberTestMap, cacheUpperKey, cacheUpperKey + 100);

        assertTrueEventually(() -> assertThat(cacheStats.getInvalidationRequests()).isEqualTo(1200));
        assertTrueEventually(() -> assertThat(cacheStats.getInvalidations()).isEqualTo(100));
        assertTrueEventually(() -> assertThat(cacheStats.getOwnedEntryCount()).isEqualTo(cacheCapacity - 100));

        getCacheRange(clientTestMap);

        assertTrueEventually(() -> assertThat(cacheStats.getOwnedEntryCount()).isEqualTo(cacheCapacity));
        assertTrueEventually(() -> assertThat(cacheStats.getMisses()).isEqualTo(cacheCapacity + 100));
        assertTrueEventually(() -> assertThat(cacheStats.getHits()).isEqualTo(cacheCapacity - 100));
    }

    private void putRange(IMap<Integer, Integer> m, int from, int to) {
        IntStream.range(from, to).forEach(n -> m.put(n, 2 * n));
    }

    private void getCacheRange(IMap<Integer, Integer> m) {
        IntStream.range(cacheLowerKey, cacheUpperKey).forEach(m::get);
    }
}
