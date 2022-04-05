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

import com.hazelcast.cache.CachePartitionLostListenerTest.EventCollectingCachePartitionLostListener;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.event.CachePartitionLostEvent;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.AbstractPartitionLostListenerTest;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(SlowTest.class)
public class CachePartitionLostListenerStressTest extends AbstractPartitionLostListenerTest {

    @Parameters(name = "numberOfNodesToCrash:{0},withData:{1},nodeLeaveType:{2},shouldExpectPartitionLostEvents:{3}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {1, true, NodeLeaveType.SHUTDOWN, false},
                {1, true, NodeLeaveType.TERMINATE, true},
                {1, false, NodeLeaveType.SHUTDOWN, false},
                {1, false, NodeLeaveType.TERMINATE, true},
                {2, true, NodeLeaveType.SHUTDOWN, false},
                {2, true, NodeLeaveType.TERMINATE, true},
                {2, false, NodeLeaveType.SHUTDOWN, false},
                {2, false, NodeLeaveType.TERMINATE, true},
                {3, true, NodeLeaveType.SHUTDOWN, false},
                {3, true, NodeLeaveType.TERMINATE, true},
                {3, false, NodeLeaveType.SHUTDOWN, false},
                {3, false, NodeLeaveType.TERMINATE, true},
        });
    }

    @Parameter(0)
    public int numberOfNodesToCrash;

    @Parameter(1)
    public boolean withData;

    @Parameter(2)
    public NodeLeaveType nodeLeaveType;

    @Parameter(3)
    public boolean shouldExpectPartitionLostEvents;

    protected int getNodeCount() {
        return 5;
    }

    protected int getCacheEntryCount() {
        return 10000;
    }

    @Test
    public void testCachePartitionLostListener() {
        List<HazelcastInstance> instances = getCreatedInstancesShuffledAfterWarmedUp();

        List<HazelcastInstance> survivingInstances = new ArrayList<HazelcastInstance>(instances);
        List<HazelcastInstance> terminatingInstances = survivingInstances.subList(0, numberOfNodesToCrash);
        survivingInstances = survivingInstances.subList(numberOfNodesToCrash, instances.size());

        HazelcastInstance instance = survivingInstances.get(0);
        HazelcastServerCachingProvider cachingProvider = createServerCachingProvider(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        List<EventCollectingCachePartitionLostListener> listeners = registerListeners(cacheManager);

        if (withData) {
            for (int i = 0; i < getNodeCount(); i++) {
                Cache<Integer, Integer> cache = cacheManager.getCache(getIthCacheName(i));
                for (int j = 0; j < getCacheEntryCount(); j++) {
                    cache.put(j, j);
                }
            }
        }

        final String log = "Surviving: " + survivingInstances + " Terminating: " + terminatingInstances;
        Map<Integer, Integer> survivingPartitions = getMinReplicaIndicesByPartitionId(survivingInstances);

        stopInstances(terminatingInstances, nodeLeaveType);
        waitAllForSafeState(survivingInstances);

        if (shouldExpectPartitionLostEvents) {
            for (int i = 0; i < getNodeCount(); i++) {
                assertListenerInvocationsEventually(log, i, numberOfNodesToCrash, listeners.get(i), survivingPartitions);
            }
        } else {
            for (final EventCollectingCachePartitionLostListener listener : listeners) {
                assertTrueAllTheTime(new AssertTask() {
                    @Override
                    public void run()
                            throws Exception {
                        assertTrue(listener.getEvents().isEmpty());
                    }
                }, 1);
            }
        }


        for (int i = 0; i < getNodeCount(); i++) {
            cacheManager.destroyCache(getIthCacheName(i));
        }
        cacheManager.close();
        cachingProvider.close();
    }

    private List<EventCollectingCachePartitionLostListener> registerListeners(CacheManager cacheManager) {
        CacheConfig<Integer, Integer> config = new CacheConfig<Integer, Integer>();
        List<EventCollectingCachePartitionLostListener> listeners = new ArrayList<EventCollectingCachePartitionLostListener>();
        for (int i = 0; i < getNodeCount(); i++) {
            EventCollectingCachePartitionLostListener listener = new EventCollectingCachePartitionLostListener(i);
            listeners.add(listener);
            config.setBackupCount(i);
            Cache<Integer, Integer> cache = cacheManager.createCache(getIthCacheName(i), config);
            ICache iCache = cache.unwrap(ICache.class);
            iCache.addPartitionLostListener(listener);
        }
        return listeners;
    }

    private void assertListenerInvocationsEventually(final String log, final int index, final int numberOfNodesToCrash,
                                                     final EventCollectingCachePartitionLostListener listener,
                                                     final Map<Integer, Integer> survivingPartitions) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                if (index < numberOfNodesToCrash) {
                    assertLostPartitions(log, listener, survivingPartitions);
                } else {
                    String message = log + " listener-" + index + " should not be invoked!";
                    assertTrue(message, listener.getEvents().isEmpty());
                }
            }
        });
    }

    private void assertLostPartitions(String log, EventCollectingCachePartitionLostListener listener,
                                      Map<Integer, Integer> survivingPartitions) {
        List<CachePartitionLostEvent> events = listener.getEvents();
        assertFalse(survivingPartitions.isEmpty());

        for (CachePartitionLostEvent event : events) {
            int failedPartitionId = event.getPartitionId();
            Integer survivingReplicaIndex = survivingPartitions.get(failedPartitionId);
            if (survivingReplicaIndex != null) {
                String message = log + ", PartitionId: " + failedPartitionId
                        + " SurvivingReplicaIndex: " + survivingReplicaIndex + " Event: " + event.toString();
                assertTrue(message, survivingReplicaIndex > listener.getBackupCount());
            }
        }
    }
}
