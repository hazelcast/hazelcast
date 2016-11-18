package com.hazelcast.map.impl.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.util.IterationType.ENTRY;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapQueryEngineImpl_queryLocalPartitions_resultSizeNoLimitTest extends HazelcastTestSupport {

    private IMap<Object, Object> map;
    private MapQueryEngineImpl queryEngine;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        map = hz.getMap(randomName());

        MapService mapService = getNodeEngineImpl(hz).getService(MapService.SERVICE_NAME);
        queryEngine = new MapQueryEngineImpl(mapService.getMapServiceContext());
    }

    @Test
    public void checkResultSize() throws Exception {
        fillMap(10000);

        Query query = Query.of().mapName(map.getName()).predicate(TruePredicate.INSTANCE).iterationType(ENTRY).build();
        QueryResult result = (QueryResult) queryEngine.execute(query, Target.LOCAL_NODE);

        assertEquals(10000, result.size());
    }

    private void fillMap(long count) {
        for (long i = 0; i < count; i++) {
            map.put(i + randomString(), "");
        }
    }
}
