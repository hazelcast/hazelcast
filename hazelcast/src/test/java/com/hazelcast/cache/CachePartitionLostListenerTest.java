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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.CachePartitionEventData;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.event.CachePartitionLostEvent;
import com.hazelcast.cache.impl.event.CachePartitionLostEventFilter;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.DataReader;
import com.hazelcast.internal.nio.DataWriter;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.IPartitionLostEvent;
import com.hazelcast.internal.partition.PartitionLostEventImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.AbstractPartitionLostListenerTest;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CachePartitionLostListenerTest extends AbstractPartitionLostListenerTest {

    @Override
    public int getNodeCount() {
        return 2;
    }

    public static class EventCollectingCachePartitionLostListener implements CachePartitionLostListener {

        private final List<CachePartitionLostEvent> events
                = Collections.synchronizedList(new LinkedList<CachePartitionLostEvent>());

        private final int backupCount;

        public EventCollectingCachePartitionLostListener(int backupCount) {
            this.backupCount = backupCount;
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

        public int getBackupCount() {
            return backupCount;
        }
    }

    @Test
    public void test_partitionLostListenerInvoked() {
        List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp(1);
        final HazelcastInstance instance = instances.get(0);

        HazelcastServerCachingProvider cachingProvider = createServerCachingProvider(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        CacheConfig<Integer, String> config = new CacheConfig<Integer, String>();
        Cache<Integer, String> cache = cacheManager.createCache(getIthCacheName(0), config);
        ICache iCache = cache.unwrap(ICache.class);

        final EventCollectingCachePartitionLostListener listener = new EventCollectingCachePartitionLostListener(0);
        iCache.addPartitionLostListener(listener);

        final IPartitionLostEvent internalEvent = new PartitionLostEventImpl(1, 1, null);
        CacheService cacheService = getNode(instance).getNodeEngine().getService(CacheService.SERVICE_NAME);
        cacheService.onPartitionLost(internalEvent);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                List<CachePartitionLostEvent> events = listener.getEvents();

                assertEquals(1, events.size());
                CachePartitionLostEvent event = events.get(0);
                assertEquals(internalEvent.getPartitionId(), event.getPartitionId());
                assertEquals(getIthCacheName(0), event.getSource());
                assertEquals(getIthCacheName(0), event.getName());
                assertEquals(instance.getCluster().getLocalMember(), event.getMember());
                assertEquals(CacheEventType.PARTITION_LOST, event.getEventType());
            }
        });

        cacheManager.destroyCache(getIthCacheName(0));
        cacheManager.close();
        cachingProvider.close();
    }

    @Test
    public void test_partitionLostListenerInvoked_whenNodeCrashed() {
        List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp(2);
        HazelcastInstance survivingInstance = instances.get(0);
        HazelcastInstance terminatingInstance = instances.get(1);

        HazelcastServerCachingProvider cachingProvider = createServerCachingProvider(survivingInstance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        CacheConfig<Integer, String> config = new CacheConfig<Integer, String>();
        config.setBackupCount(0);
        Cache<Integer, String> cache = cacheManager.createCache(getIthCacheName(0), config);
        ICache iCache = cache.unwrap(ICache.class);

        final EventCollectingCachePartitionLostListener listener = new EventCollectingCachePartitionLostListener(0);
        iCache.addPartitionLostListener(listener);

        final Set<Integer> survivingPartitionIds = new HashSet<Integer>();
        Node survivingNode = getNode(survivingInstance);
        Address survivingAddress = survivingNode.getThisAddress();

        for (IPartition partition : survivingNode.getPartitionService().getPartitions()) {
            if (survivingAddress.equals(partition.getReplicaAddress(0))) {
                survivingPartitionIds.add(partition.getPartitionId());
            }
        }

        terminatingInstance.getLifecycleService().terminate();
        waitAllForSafeState(survivingInstance);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final List<CachePartitionLostEvent> events = listener.getEvents();
                assertFalse(events.isEmpty());
                for (CachePartitionLostEvent event : events) {
                    assertFalse(survivingPartitionIds.contains(event.getPartitionId()));
                }
            }
        });

        cacheManager.destroyCache(getIthCacheName(0));
        cacheManager.close();
        cachingProvider.close();
    }

    @Test
    public void test_cachePartitionEventData_serialization() throws IOException {
        CachePartitionEventData cachePartitionEventData = new CachePartitionEventData("cacheName", 1, null);
        ObjectDataOutput output = mock(ObjectDataOutput.class,
                withSettings().extraInterfaces(DataWriter.class));
        cachePartitionEventData.writeData(output);

        verify(output).writeString("cacheName");
        verify(output).writeInt(1);
    }

    @Test
    public void test_cachePartitionEventData_deserialization() throws IOException {
        CachePartitionEventData cachePartitionEventData = new CachePartitionEventData("", 0, null);

        ObjectDataInput input = mock(ObjectDataInput.class,
                withSettings().extraInterfaces(DataReader.class));
        when(input.readString()).thenReturn("cacheName");
        when(input.readInt()).thenReturn(1);

        cachePartitionEventData.readData(input);

        assertEquals("cacheName", cachePartitionEventData.getName());
        assertEquals(1, cachePartitionEventData.getPartitionId());
    }

    @Test
    public void testCachePartitionLostEventFilter() {
        CachePartitionLostEventFilter filter = new CachePartitionLostEventFilter();
        assertEquals(new CachePartitionLostEventFilter(), filter);
        assertFalse(filter.eval(null));
    }
}
