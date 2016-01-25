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
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.util.IterationType.ENTRY;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelTest.class})
public class MapQueryEngineImpl_queryLocalPartitions_resultSizeLimitTest extends HazelcastTestSupport {

    private static final long RESULT_SIZE_LIMIT = QueryResultSizeLimiter.MINIMUM_MAX_RESULT_LIMIT + 4223;
    private static final int PARTITION_COUNT = 271;

    private IMap<String, String> map;
    private MapQueryEngineImpl queryEngine;
    private int limit;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT, "" + PARTITION_COUNT);
        config.setProperty(GroupProperty.QUERY_RESULT_SIZE_LIMIT, "" + RESULT_SIZE_LIMIT);

        HazelcastInstance hz = createHazelcastInstance(config);
        map = hz.getMap(randomName());

        MapService mapService = getNodeEngineImpl(hz).getService(MapService.SERVICE_NAME);
        queryEngine = new MapQueryEngineImpl(mapService.getMapServiceContext(), new RuleBasedQueryOptimizer());

        // we fill all partitions, so we get the NodeResultLimit for all partitions as well
        QueryResultSizeLimiter resultSizeLimiter = queryEngine.getQueryResultSizeLimiter();
        limit = (int) resultSizeLimiter.getNodeResultLimit(PARTITION_COUNT);
    }

    @Test
    public void checkResultLimit() throws Exception {
        QueryResult result = queryEngine.queryLocalPartitions(map.getName(), TruePredicate.INSTANCE, ENTRY);

        assertEquals(limit, result.getResultLimit());
    }

    @Test
    public void checkResultSize_limitNotExceeded() throws Exception {
        fillMap(limit - 1);

        QueryResult result = queryEngine.queryLocalPartitions(map.getName(), TruePredicate.INSTANCE, ENTRY);

        assertEquals(limit - 1, result.getRows().size());
    }

    @Test
    public void checkResultSize_limitEquals() throws Exception {
        fillMap(limit);

        QueryResult result = queryEngine.queryLocalPartitions(map.getName(), TruePredicate.INSTANCE, ENTRY);

        assertEquals(limit, result.getRows().size());
    }

    @Test(expected = QueryResultSizeExceededException.class)
    public void checkResultSize_limitExceeded() throws Exception {
        fillMap(limit + 1);

        queryEngine.queryLocalPartitions(map.getName(), TruePredicate.INSTANCE, ENTRY);
    }

    private void fillMap(long count) {
        for (long i = 0; i < count; i++) {
            map.put(i + randomString(), "");
        }
    }
}
