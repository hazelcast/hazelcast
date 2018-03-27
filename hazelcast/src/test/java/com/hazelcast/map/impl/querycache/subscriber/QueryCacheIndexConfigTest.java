package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryCacheIndexConfigTest extends HazelcastTestSupport {

    @Test
    public void testIndexConfigIsRespected() {
        final Config config = new Config();
        final MapConfig mapConfig = new MapConfig("map").addQueryCacheConfig(
                new QueryCacheConfig().setName("query-cache").setPredicateConfig(new PredicateConfig(TruePredicate.INSTANCE))
                                      .addIndexConfig(new MapIndexConfig("field", true)));
        config.addMapConfig(mapConfig);

        final HazelcastInstance instance = createHazelcastInstance(config);
        final IMap<Object, Object> map = instance.getMap("map");
        final DefaultQueryCache<Object, Object> cache = (DefaultQueryCache<Object, Object>) map.getQueryCache("query-cache");

        assertNotNull(cache.indexes.getIndex("field"));
        assertTrue(cache.indexes.getIndex("field").isOrdered());
    }

}
