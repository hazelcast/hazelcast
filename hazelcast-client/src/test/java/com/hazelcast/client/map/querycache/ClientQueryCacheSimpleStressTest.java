package com.hazelcast.client.map.querycache;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
public class ClientQueryCacheSimpleStressTest extends HazelcastTestSupport {

    private final String mapName = randomString();
    private final String cacheName = randomString();
    private final ClientConfig config = new ClientConfig();
    private final int numberOfElementsToPut = 10000;
    private HazelcastInstance instance;

    @Before
    public void setUp() {
        Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();

        setQueryCacheConfig();

        instance = HazelcastClient.newHazelcastClient(config);
    }

    private void setQueryCacheConfig() {
        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
        queryCacheConfig
                .setBufferSize(30)
                .setDelaySeconds(2)
                .setBatchSize(2)
                .setPopulate(true)
                .getPredicateConfig().setImplementation(TruePredicate.INSTANCE);

        config.addQueryCacheConfig(mapName, queryCacheConfig);
    }

    @After
    public void tearDown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testStress() throws Exception {
        final IMap<Integer, Integer> map = instance.getMap(mapName);

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < numberOfElementsToPut; i++) {
                    map.put(i, i);
                }
            }
        };

        Thread thread = new Thread(runnable);
        thread.start();

        QueryCache<Integer, Integer> queryCache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);

        thread.join();

        assertQueryCacheSizeEventually(numberOfElementsToPut, queryCache);
    }

    private void assertQueryCacheSizeEventually(final int expected, final QueryCache queryCache) {
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expected, queryCache.size());
            }
        };

        assertTrueEventually(task);
    }
}
