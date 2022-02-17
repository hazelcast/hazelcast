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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.map.QueryCache;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.map.impl.querycache.AbstractQueryCacheTestSupport.getMap;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheMapLoaderTest extends HazelcastTestSupport {

    @Parameterized.Parameter(0)
    public MapLoader mapLoader;

    @Parameterized.Parameter(1)
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameters(name = "{0}, inMemoryFormat {1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {new DefaultMapLoader(), BINARY},
                {new SlowMapLoader(1), BINARY},
                {new DefaultMapLoader(), OBJECT},
                {new SlowMapLoader(1), OBJECT}
        });
    }

    @SuppressWarnings("unchecked")
    private static final Predicate<Integer, Integer> TRUE_PREDICATE = Predicates.alwaysTrue();

    @Test
    public void testQueryCache_includesLoadedEntries_after_get() {
        String mapName = randomString();
        String cacheName = randomString();

        Config config = getConfig(mapName, cacheName);
        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = getMap(node, mapName);

        final QueryCache<Integer, Integer> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);

        map.get(1);
        map.get(2);
        map.get(3);

        assertTrueEventually(() -> assertEquals(3, cache.size()));
    }

    @Test
    public void testQueryCache_includesLoadedEntries_after_getAll() {
        String mapName = randomString();
        String cacheName = randomString();

        Config config = getConfig(mapName, cacheName);
        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = getMap(node, mapName);

        final QueryCache<Integer, Integer> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);

        map.getAll(new HashSet<>(asList(1, 2, 3)));

        assertTrueEventually(() -> assertEquals(3, cache.size()));
    }

    private Config getConfig(String mapName, String cacheName) {
        Config config = getConfig();

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setInMemoryFormat(inMemoryFormat);
        mapConfig.getMapStoreConfig()
                .setEnabled(true)
                .setImplementation(mapLoader);

        QueryCacheConfig cacheConfig = new QueryCacheConfig(cacheName);
        mapConfig.addQueryCacheConfig(cacheConfig);
        return config;
    }

    static class DefaultMapLoader extends MapStoreAdapter<Integer, Integer> {

        private final ConcurrentMap<Integer, Integer> map = new ConcurrentHashMap<Integer, Integer>();

        DefaultMapLoader() {
            map.put(1, 1);
            map.put(2, 2);
            map.put(3, 4);
        }

        @Override
        public Integer load(Integer key) {
            return map.get(key);
        }

        @Override
        public Map<Integer, Integer> loadAll(Collection<Integer> keys) {
            Map<Integer, Integer> results = new HashMap<Integer, Integer>();
            for (Integer key : keys) {
                results.put(key, map.get(key));
            }
            return results;
        }

        @Override
        public Iterable<Integer> loadAllKeys() {
            return Collections.emptySet();
        }

        @Override
        public String toString() {
            return "TestMapLoader";
        }
    }

    /**
     * for every method call, first sleep then do method call.
     */
    static class SlowMapLoader extends DefaultMapLoader {

        private final int sleepSeconds;

        SlowMapLoader(int sleepSeconds) {
            this.sleepSeconds = sleepSeconds;
        }

        @Override
        public Integer load(Integer key) {
            sleepSeconds(sleepSeconds);

            return super.load(key);
        }

        @Override
        public Map<Integer, Integer> loadAll(Collection<Integer> keys) {
            sleepSeconds(sleepSeconds);

            return super.loadAll(keys);
        }

        @Override
        public Iterable<Integer> loadAllKeys() {
            sleepSeconds(sleepSeconds);

            return super.loadAllKeys();
        }

        @Override
        public String toString() {
            return "SlowMapLoader";
        }
    }
}
