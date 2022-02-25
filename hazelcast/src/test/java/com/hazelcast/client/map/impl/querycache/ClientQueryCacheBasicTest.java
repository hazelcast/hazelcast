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
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.AbstractEntryEventTypesTest.Person;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.event.MapEventPublisherImpl;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;

import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

/**
 * Test basic QueryCache operation: create a map, put/update/remove values and assert size of query cache.
 * Parametrized with QueryCache option includeValues true/false & using default and query-cache-natural filtering strategies.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientQueryCacheBasicTest extends HazelcastTestSupport {

    private static final String TEST_MAP_NAME = "ClientQueryCacheBasicTestMap";
    private static final String QUERY_CACHE_NAME = "query-cache";

    private TestHazelcastFactory factory;
    private IMap<Integer, Person> map;
    private QueryCache queryCache;
    private Predicate predicate = Predicates.sql("age > 50");

    @Parameterized.Parameter
    public boolean includeValues;

    @Parameterized.Parameter(1)
    public boolean useQueryCacheNaturalFilteringStrategy;

    @Parameterized.Parameter(2)
    public boolean useNearCache;

    @Parameterized.Parameters(name = "includeValues: {0}, useQueryCacheFilteringStrategy: {1}, nearCache: {2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {false, false, false},
                {false, false, true},
                {false, true, false},
                {false, true, true},
                {true, false, false},
                {true, false, true},
                {true, true, false},
                {true, true, true},
        });
    }

    // setup a map with 2 query caches, same predicate, one includes values, the other excludes values
    @Before
    public void setup() {
        Config config = getConfig();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addQueryCacheConfig(TEST_MAP_NAME, new QueryCacheConfig(QUERY_CACHE_NAME)
                .setPredicateConfig(new PredicateConfig(predicate))
                .setIncludeValue(includeValues));
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        if (useNearCache) {
            clientConfig.addNearCacheConfig(new NearCacheConfig()
                    .setName(TEST_MAP_NAME)
                    .setInvalidateOnChange(true));
        }
        clientConfig.setProperty(MapEventPublisherImpl.LISTENER_WITH_PREDICATE_PRODUCES_NATURAL_EVENT_TYPES.getName(),
                Boolean.toString(useQueryCacheNaturalFilteringStrategy));

        factory = new TestHazelcastFactory();
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        map = client.getMap(TEST_MAP_NAME);
        queryCache = map.getQueryCache(QUERY_CACHE_NAME);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void entryAdded_whenValueMatchesPredicate() {
        map.put(1, new Person("a", 75));
        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return queryCache.size();
            }
        }, 1);
    }

    @Test
    public void entryAdded_whenValueOutsidePredicate() {
        // when a value not matching predicate is put
        map.put(1, new Person("a", 25));
        // then querycache does not contain any elements
        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return queryCache.size();
            }
        }, 0);
    }

    @Test
    public void entryRemoved_whenValueMatchesPredicate() {
        // when 2 values matching predicate are put & 1 is removed
        map.put(1, new Person("a", 75));
        map.put(2, new Person("a", 95));
        map.remove(1);
        // then size of querycache is 1
        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return queryCache.size();
            }
        }, 1);
    }

    @Test
    public void entryRemoved_whenValueOutsidePredicate() {
        // when 2 values not matching predicate are put & 1 is removed
        map.put(1, new Person("a", 15));
        map.put(2, new Person("a", 25));
        map.remove(1);
        // then size of querycache is 0
        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return queryCache.size();
            }
        }, 0);
    }

    @Test
    public void entryUpdated_whenOldValueOutside_newValueMatchesPredicate() {
        // when a value not matching predicate is put and is updated to match the predicate
        map.put(1, new Person("a", 15));
        map.replace(1, new Person("a", 85));
        // then size of querycache is 1
        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return queryCache.size();
            }
        }, 1);
    }

    @Test
    public void entryUpdated_whenOldValueOutside_newValueOutsidePredicate() {
        // when a value not matching predicate is put and is updated to another value that does not match the predicate
        map.put(1, new Person("a", 15));
        map.replace(1, new Person("a", 25));
        // then size of querycache is 0
        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return queryCache.size();
            }
        }, 0);
    }

    @Test
    public void entryUpdated_whenOldValueMatches_newValueMatchesPredicate() {
        // when a value matching predicate is put and is updated to another value that matches the predicate
        map.put(1, new Person("a", 55));
        map.replace(1, new Person("a", 56));
        // then size of querycache is 1
        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return queryCache.size();
            }
        }, 1);
    }

    @Test
    public void entryUpdated_whenOldValueMatches_newValueOutsidePredicate() {
        // when a value matching predicate is put and is updated to another value that does not match the predicate
        map.put(1, new Person("a", 55));
        map.replace(1, new Person("a", 15));
        // then size of querycache is 0
        assertEqualsEventually(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return queryCache.size();
            }
        }, 0);
    }
}
