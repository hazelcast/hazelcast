package com.hazelcast.map.impl.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.predicates.RuleBasedQueryOptimizer;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.util.IterationType;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MapQueryEngineImpl_queryLocalPartitions_resultSizeNoLimitTest extends HazelcastTestSupport {

    private IMap<Object, Object> map;
    private MapQueryEngineImpl queryEngine;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        map = hz.getMap(randomName());

        MapService mapService = getNodeEngineImpl(hz).getService(MapService.SERVICE_NAME);
        queryEngine = new MapQueryEngineImpl(mapService.getMapServiceContext(), new RuleBasedQueryOptimizer());
    }

    @Test
    public void checkResultLimit() throws Exception {
        QueryResult result = queryEngine.queryLocalPartitions(map.getName(), TruePredicate.INSTANCE, IterationType.ENTRY);

        assertEquals(Long.MAX_VALUE, result.getResultLimit());
    }
}
