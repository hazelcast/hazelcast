package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.predicates.RuleBasedQueryOptimizer;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.IterationType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
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
        QueryResult result = queryEngine.queryLocalPartitions(map.getName(), TruePredicate.INSTANCE, IterationType.ENTRY);

        assertEquals(limit, result.getResultLimit());
    }

    @Test
    public void whenResultSizeLimitNotExceeded() throws Exception {
        fillMap(limit - 1);

        QueryResult result = queryEngine.queryLocalPartitions(map.getName(), TruePredicate.INSTANCE, IterationType.ENTRY);

        assertEquals(limit - 1, result.getRows().size());
    }

    @Test
    public void whenResultSizeLimitEquals() throws Exception {
        fillMap(limit);

        QueryResult result = queryEngine.queryLocalPartitions(map.getName(), TruePredicate.INSTANCE, IterationType.ENTRY);

        assertEquals(limit, result.getRows().size());
    }

    @Test(expected = QueryResultSizeExceededException.class)
    public void whenResultSizeLimitExceeded() throws Exception {
        fillMap(limit + 1);

        queryEngine.queryLocalPartitions(map.getName(), TruePredicate.INSTANCE, IterationType.ENTRY);
    }

    private void fillMap(long count) {
        for (long k = 0; k < count; k++) {
            map.put(k + randomString(), "");
        }
    }
}
