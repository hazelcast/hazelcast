/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.map.querycache;

import com.hazelcast.client.impl.querycache.ClientQueryCacheContext;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheEndToEndProvider;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheFactory;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberContext;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientQueryCacheMemoryLeakTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @Test
    public void removes_internal_query_caches_upon_map_destroy() throws Exception {
        factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient();

        String mapName = "test";
        IMap<Integer, Integer> map = client.getMap(mapName);

        for (int j = 0; j < 10; j++) {
            map.getQueryCache(j + "-test-QC", TruePredicate.INSTANCE, true);
        }

        map.destroy();

        ClientQueryCacheContext queryCacheContext = ((ClientMapProxy) map).getQueryCacheContext();
        SubscriberContext subscriberContext = queryCacheContext.getSubscriberContext();
        QueryCacheEndToEndProvider provider = subscriberContext.getEndToEndQueryCacheProvider();
        QueryCacheFactory queryCacheFactory = subscriberContext.getQueryCacheFactory();

        assertEquals(0, provider.getQueryCacheCount(mapName));
        assertEquals(0, queryCacheFactory.getQueryCacheCount());
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }
}
