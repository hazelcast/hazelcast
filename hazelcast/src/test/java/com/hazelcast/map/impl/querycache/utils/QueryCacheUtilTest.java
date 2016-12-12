package com.hazelcast.map.impl.querycache.utils;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.querycache.NodeQueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.map.impl.querycache.utils.QueryCacheUtil.getAccumulatorOrNull;
import static com.hazelcast.map.impl.querycache.utils.QueryCacheUtil.getAccumulators;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryCacheUtilTest extends HazelcastTestSupport {

    private QueryCacheContext context;

    @Before
    public void setUp() {
        HazelcastInstance instance = createHazelcastInstance();
        MapService mapService = getNodeEngineImpl(instance).getService(MapService.SERVICE_NAME);

        context = new NodeQueryCacheContext(mapService.getMapServiceContext());
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(QueryCacheUtil.class);
    }

    @Test
    public void getAccumulators_whenNoAccumulatorsRegistered_thenReturnEmptyMap() {
        Map<Integer, Accumulator> accumulators = getAccumulators(context, "myMap", "myCache");

        assertNotNull(accumulators);
        assertEquals(0, accumulators.size());
    }

    @Test
    public void getAccumulatorOrNull_whenNoAccumulatorsRegistered_thenReturnNull() {
        Accumulator accumulator = getAccumulatorOrNull(context, "myMap", "myCache", -1);

        assertNull(accumulator);
    }
}
