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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.EventLostEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.listener.EventLostListener;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.map.impl.querycache.AbstractQueryCacheTestSupport.getMap;
import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheRecoveryUponEventLossTest extends HazelcastTestSupport {

    @Test
    public void testForceConsistency() {
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(3);

        final String mapName = randomString();
        String queryCacheName = randomString();

        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        queryCacheConfig.setBatchSize(1111);
        queryCacheConfig.setDelaySeconds(3);

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.addQueryCacheConfig(queryCacheConfig);
        mapConfig.setBackupCount(0);

        final HazelcastInstance node = instanceFactory.newHazelcastInstance(config);
        HazelcastInstance node2 = instanceFactory.newHazelcastInstance(config);
        setTestSequencer(node, 9);
        setTestSequencer(node2, 9);

        IMap<Integer, Integer> map = getMap(node, mapName);
        node2.getMap(mapName);

        final CountDownLatch waitEventLossNotification = new CountDownLatch(1);
        final QueryCache queryCache = map.getQueryCache(queryCacheName, Predicates.sql("this > 20"), true);
        queryCache.addEntryListener(new EventLostListener() {
            @Override
            public void eventLost(EventLostEvent event) {
                queryCache.tryRecover();

                waitEventLossNotification.countDown();
            }
        }, false);

        int count = 30;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        assertOpenEventually(waitEventLossNotification);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(9, queryCache.size());
            }
        });

        // re-put entries and check if broken-sequences holder map will be empty
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                Map brokenSequences = getBrokenSequences(node, mapName, queryCache);
                assertTrue("After recovery, there should be no broken sequences left",
                        brokenSequences.isEmpty());
            }
        });
    }

    private void setTestSequencer(HazelcastInstance instance, int eventCount) {
        Node node = getNode(instance);
        MapService service = node.getNodeEngine().getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        queryCacheContext.setSubscriberContext(new TestSubscriberContext(queryCacheContext, eventCount, true));
    }

    private Map getBrokenSequences(HazelcastInstance instance, String mapName, QueryCache queryCache) {
        Node node = getNode(instance);
        MapService service = node.getNodeEngine().getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        QueryCacheContext context = mapServiceContext.getQueryCacheContext();

        SubscriberContext subscriberContext = context.getSubscriberContext();
        MapSubscriberRegistry mapSubscriberRegistry = subscriberContext.getMapSubscriberRegistry();

        SubscriberRegistry subscriberRegistry = mapSubscriberRegistry.getOrNull(mapName);
        if (subscriberRegistry == null) {
            return Collections.emptyMap();
        }

        String cacheId = ((InternalQueryCache) queryCache).getCacheId();
        Accumulator accumulator = subscriberRegistry.getOrNull(cacheId);
        if (accumulator == null) {
            return Collections.emptyMap();
        }

        SubscriberAccumulator subscriberAccumulator = (SubscriberAccumulator) accumulator;
        return subscriberAccumulator.getBrokenSequences();
    }
}
