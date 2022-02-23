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

package com.hazelcast.client.map.impl.querycache;

import com.hazelcast.client.map.impl.querycache.subscriber.TestClientSubscriberContext;
import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.EventLostEvent;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.listener.EventLostListener;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientQueryCacheEventLostListenerTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastInstance node;

    @Before
    public void setUp() {
        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");

        node = factory.newHazelcastInstance(config);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testListenerNotified_onEventLoss() throws Exception {
        int count = 30;
        String mapName = randomString();
        String queryCacheName = randomString();

        IMap<Integer, Integer> mapNode = node.getMap(mapName);

        HazelcastInstance client = factory.newHazelcastClient();

        IMap<Integer, Integer> mapClient = client.getMap(mapName);
        setTestSequencer(mapClient, 9);

        // expecting one lost event publication per partition.
        final CountDownLatch lostEventCount = new CountDownLatch(1);
        QueryCache queryCache = mapClient.getQueryCache(queryCacheName, Predicates.sql("this > 20"), true);
        queryCache.addEntryListener(new EventLostListener() {
            @Override
            public void eventLost(EventLostEvent event) {
                lostEventCount.countDown();
            }
        }, false);


        for (int i = 0; i < count; i++) {
            mapNode.put(i, i);
        }

        assertOpenEventually(lostEventCount);
    }

    private void setTestSequencer(IMap map, int eventCount) {
        ClientMapProxy proxy = (ClientMapProxy) map;
        QueryCacheContext queryCacheContext = proxy.getQueryCacheContext();
        queryCacheContext.setSubscriberContext(new TestClientSubscriberContext(queryCacheContext, eventCount, true));
    }
}
