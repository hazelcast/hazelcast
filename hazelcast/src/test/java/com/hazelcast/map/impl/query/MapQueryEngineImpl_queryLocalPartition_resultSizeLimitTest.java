package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.predicates.RuleBasedQueryOptimizer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.IterationType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapQueryEngineImpl_queryLocalPartition_resultSizeLimitTest extends HazelcastTestSupport {

    private static final int RESULT_SIZE_LIMIT = QueryResultSizeLimiter.MINIMUM_MAX_RESULT_LIMIT + 2342;
    private static final int PARTITION_ID = 0;

    private HazelcastInstance hz;
    private IMap<String, String> map;
    private MapQueryEngineImpl queryEngine;
    private int limit;

    @Before
    public void setup() {
        Config config = new Config();
        // we reduce the number of partitions to speed up content generation
        config.setProperty(GroupProperty.PARTITION_COUNT, "2");
        config.setProperty(GroupProperty.QUERY_RESULT_SIZE_LIMIT, "" + RESULT_SIZE_LIMIT);

        hz = createHazelcastInstance(config);
        map = hz.getMap(randomName());

        MapService mapService = getNodeEngineImpl(hz).getService(MapService.SERVICE_NAME);
        queryEngine = new MapQueryEngineImpl(mapService.getMapServiceContext(), new RuleBasedQueryOptimizer());

        // we just fill a single partition, so we get the NodeResultLimit for a single partition as well
        QueryResultSizeLimiter resultSizeLimiter = queryEngine.getQueryResultSizeLimiter();
        limit = (int) resultSizeLimiter.getNodeResultLimit(1);
    }

    @Test
    public void whenLimitNotExceeded() throws Exception {
        fillPartition(limit - 1);

        QueryResult result = queryEngine.queryLocalPartition(map.getName(), TruePredicate.INSTANCE, PARTITION_ID, IterationType.ENTRY);

        assertEquals(limit - 1, result.getRows().size());
    }

    @Test
    public void whenLimitEquals() throws Exception {
        fillPartition(limit);

        QueryResult result = queryEngine.queryLocalPartition(map.getName(), TruePredicate.INSTANCE, PARTITION_ID, IterationType.ENTRY);

        assertEquals(limit, result.getRows().size());
    }

    @Test(expected = QueryResultSizeExceededException.class)
    public void whenLimitExceeded() throws Exception {
        fillPartition(limit + 1);

        queryEngine.queryLocalPartition(map.getName(), TruePredicate.INSTANCE, PARTITION_ID, IterationType.ENTRY);
    }

    private void fillPartition(int count) {
        for (int k = 0; k < count; k++) {
            String key = generateKeyForPartition(hz, PARTITION_ID);
            map.put(key, "");
        }
    }
}
