/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.spi.impl.listener.ClientEventRegistration;
import com.hazelcast.client.spi.impl.listener.ClientRegistrationKey;
import com.hazelcast.client.spi.impl.listener.SmartClientListenerService;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.nio.Connection;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientQueryCacheDestroyResourcesTest extends HazelcastTestSupport {

    private static final String MAP_NAME_1 = "ClientQueryCacheClientContextTest-1";
    private static final String MAP_NAME_2 = "ClientQueryCacheClientContextTest-2";
    private static final String MAP_NAME_3 = "ClientQueryCacheClientContextTest-3";
    private static final String QUERY_CACHE_NAME_1 = "query-cache-1";
    private static final String QUERY_CACHE_NAME_2 = "query-cache-2";
    private static final String QUERY_CACHE_NAME_3 = "query-cache-3";

    private TestHazelcastFactory factory;
    private IMap<String, String> map1;
    private IMap<String, String> map2;
    private IMap<String, String> map3;
    private Predicate predicate = TruePredicate.INSTANCE;
    private HazelcastInstance clientInstance;

    @Before
    public void setup() {
        Config config = getConfig();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addQueryCacheConfig(MAP_NAME_1, newQueryCacheConfig(QUERY_CACHE_NAME_1));
        clientConfig.addQueryCacheConfig(MAP_NAME_2, newQueryCacheConfig(QUERY_CACHE_NAME_2));
        clientConfig.addQueryCacheConfig(MAP_NAME_3, newQueryCacheConfig(QUERY_CACHE_NAME_3));

        factory = new TestHazelcastFactory();
        factory.newHazelcastInstance(config);
        clientInstance = factory.newHazelcastClient(clientConfig);
        map1 = clientInstance.getMap(MAP_NAME_1);
        map2 = clientInstance.getMap(MAP_NAME_2);
        map3 = clientInstance.getMap(MAP_NAME_3);
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
    public void destroy_deregisters_listeners() throws Exception {
        QueryCache<String, String> queryCache1 = map1.getQueryCache(QUERY_CACHE_NAME_1);
        QueryCache<String, String> queryCache2 = map2.getQueryCache(QUERY_CACHE_NAME_2);
        QueryCache<String, String> queryCache3 = map3.getQueryCache(QUERY_CACHE_NAME_3);

        queryCache1.destroy();
        queryCache2.destroy();
        queryCache3.destroy();

        SmartClientListenerService smartListenerService = getSmartListenerService();
        Map<ClientRegistrationKey, Map<Connection, ClientEventRegistration>> registrations
                = smartListenerService.getRegistrations();

        assertEquals(registrations.toString(), 0, registrations.size());
    }

    protected SmartClientListenerService getSmartListenerService() {
        return (SmartClientListenerService) ((HazelcastClientProxy) clientInstance).client.getListenerService();
    }
}
