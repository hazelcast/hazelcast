package com.hazelcast.map.impl.querycache;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.mapreduce.helpers.Employee;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.impl.querycache.AbstractQueryCacheTestSupport.getMap;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryCachePredicateConfigTest extends HazelcastTestSupport {

    @Test
    public void test_whenSqlIsSet() {
        String mapName = randomString();
        String cacheName = randomString();

        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(mapName);

        QueryCacheConfig cacheConfig = new QueryCacheConfig(cacheName);
        PredicateConfig predicateConfig = cacheConfig.getPredicateConfig();
        predicateConfig.setSql("id > 10");

        mapConfig.addQueryCacheConfig(cacheConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Employee> map = getMap(node, mapName);

        for (int i = 0; i < 15; i++) {
            map.put(i, new Employee(i));
        }

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName);

        assertEquals(4, cache.size());
    }

    @Test
    public void test_whenClassNameIsSet() {
        String mapName = randomString();
        String cacheName = randomString();

        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(mapName);

        QueryCacheConfig cacheConfig = new QueryCacheConfig(cacheName);
        PredicateConfig predicateConfig = cacheConfig.getPredicateConfig();
        predicateConfig.setClassName("com.hazelcast.map.impl.querycache.TestPredicate");

        mapConfig.addQueryCacheConfig(cacheConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Employee> map = getMap(node, mapName);

        for (int i = 0; i < 15; i++) {
            map.put(i, new Employee(i));
        }

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName);

        assertEquals(0, cache.size());
    }
}
