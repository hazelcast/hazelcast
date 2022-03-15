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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientQueryCacheClientContextTest extends HazelcastTestSupport {

    private static final String MAP_NAME_1 = "ClientQueryCacheClientContextTest-1";
    private static final String MAP_NAME_2 = "ClientQueryCacheClientContextTest-2";
    private static final String MAP_NAME_3 = "ClientQueryCacheClientContextTest-3";
    private static final String QUERY_CACHE_1 = "query-cache-1";
    private static final String QUERY_CACHE_2 = "query-cache-2";
    private static final String QUERY_CACHE_3 = "query-cache-3";

    private TestHazelcastFactory factory;
    private IMap<String, String> map1;
    private IMap<String, String> map2;
    private IMap<String, String> map3;
    private Predicate predicate = Predicates.alwaysTrue();

    @Before
    public void setup() {
        Config config = getConfig();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addQueryCacheConfig(MAP_NAME_1, newQueryCacheConfig(QUERY_CACHE_1));
        clientConfig.addQueryCacheConfig(MAP_NAME_2, newQueryCacheConfig(QUERY_CACHE_2));
        clientConfig.addQueryCacheConfig(MAP_NAME_3, newQueryCacheConfig(QUERY_CACHE_3));

        factory = new TestHazelcastFactory();
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        map1 = client.getMap(MAP_NAME_1);
        map2 = client.getMap(MAP_NAME_2);
        map3 = client.getMap(MAP_NAME_3);
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    private QueryCacheConfig newQueryCacheConfig(String cacheName) {
        return new QueryCacheConfig(cacheName)
                .setPredicateConfig(new PredicateConfig(predicate));
    }

    @Test
    public void same_context_object_is_used_by_all_query_caches() throws Exception {
        ClientQueryCacheContext instance1 = getQueryCacheContext(map1);
        ClientQueryCacheContext instance2 = getQueryCacheContext(map2);
        ClientQueryCacheContext instance3 = getQueryCacheContext(map3);

        assertEquals(instance1, instance2);
        assertEquals(instance2, instance3);
    }

    private ClientQueryCacheContext getQueryCacheContext(IMap map) {
        return ((ClientMapProxy) map).getQueryCacheContext();
    }
}
