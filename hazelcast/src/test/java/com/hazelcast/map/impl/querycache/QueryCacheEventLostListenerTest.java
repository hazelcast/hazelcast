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

package com.hazelcast.map.impl.querycache;

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
import com.hazelcast.map.impl.querycache.subscriber.TestSubscriberContext;
import com.hazelcast.map.listener.EventLostListener;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.map.impl.querycache.AbstractQueryCacheTestSupport.getMap;
import static com.hazelcast.test.Accessors.getNode;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheEventLostListenerTest extends HazelcastTestSupport {

    @Test
    public void testListenerNotified_uponEventLoss() {
        String mapName = randomString();
        String queryCacheName = randomString();
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(3);
        Config config = new Config();

        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        queryCacheConfig.setBatchSize(1111);
        queryCacheConfig.setDelaySeconds(3);

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.addQueryCacheConfig(queryCacheConfig);
        mapConfig.setBackupCount(0);

        HazelcastInstance node = instanceFactory.newHazelcastInstance(config);
        HazelcastInstance node2 = instanceFactory.newHazelcastInstance(config);
        setTestSequencer(node, 9);
        setTestSequencer(node2, 9);

        IMap<Integer, Integer> map = getMap(node, mapName);
        node2.getMap(mapName);

        // set test sequencer to all nodes
        int count = 30;

        // expecting one lost event per partition
        final CountDownLatch lossCount = new CountDownLatch(1);
        final QueryCache queryCache = map.getQueryCache(queryCacheName, Predicates.sql("this > 20"), true);
        queryCache.addEntryListener(new EventLostListener() {
            @Override
            public void eventLost(EventLostEvent event) {
                lossCount.countDown();

            }
        }, false);

        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        assertOpenEventually(lossCount);
    }

    private void setTestSequencer(HazelcastInstance instance, int eventCount) {
        Node node = getNode(instance);
        MapService service = node.getNodeEngine().getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        queryCacheContext.setSubscriberContext(new TestSubscriberContext(queryCacheContext, eventCount, true));
    }
}
