package com.hazelcast.map.impl.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.IterationType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class MapQueryEngineImplTest extends HazelcastTestSupport {

    private HazelcastInstance instance;
    private IMap<String, String> map;
    private MapQueryEngine queryEngine;

    private int partitionId;
    private String key;
    private String value;

    @Before
    public void before() {
        instance = createHazelcastInstance();
        map = instance.getMap(randomName());
        MapService mapService = getNodeEngineImpl(instance).getService(MapService.SERVICE_NAME);
        queryEngine = mapService.getMapServiceContext().getMapQueryEngine(map.getName());

        partitionId = 100;
        key = generateKeyForPartition(instance, partitionId);
        value = randomString();

        map.put(key, value);
    }

    @After
    public void after() {
        if (instance != null) {
            instance.shutdown();
        }
    }

    @Test
    public void runQueryOnAllPartitions() throws ExecutionException, InterruptedException {
        Set result = queryEngine
                .runQueryOnLocalPartitions(map.getName(), Predicates.equal("this", value), IterationType.KEY, false);

        assertEquals(1, result.size());
        assertEquals(key, result.iterator().next());
    }

    @Test
    public void runQueryOnLocalPartitions() throws ExecutionException, InterruptedException {
        Set result = queryEngine
                .runQueryOnAllPartitions(map.getName(), Predicates.equal("this", value), IterationType.KEY, false);

        assertEquals(1, result.size());
        assertEquals(key, result.iterator().next());
    }

    @Test
    public void runQueryOnAllPartitions_key() throws ExecutionException, InterruptedException {
        Set result = queryEngine
                .runQueryOnLocalPartitions(map.getName(), Predicates.equal("this", value), IterationType.KEY, false);

        assertEquals(1, result.size());
        assertEquals(key, result.iterator().next());
    }

    @Test
    public void runQueryOnAllPartitions_value() throws ExecutionException, InterruptedException {
        Set result = queryEngine
                .runQueryOnLocalPartitions(map.getName(), Predicates.equal("this", value), IterationType.VALUE, false);

        assertEquals(1, result.size());
        assertEquals(value, result.iterator().next());
    }

    @Test
    public void runQueryOnGivenPartition() throws ExecutionException, InterruptedException {
        Set result = queryEngine
                .runQueryOnGivenPartition(map.getName(), Predicates.equal("this", value), IterationType.ENTRY, false, partitionId);

        assertEquals(1, result.size());
        assertEquals(map.get(key), ((Map.Entry) result.iterator().next()).getValue());
    }

}
