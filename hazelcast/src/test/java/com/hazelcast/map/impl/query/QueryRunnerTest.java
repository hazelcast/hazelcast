package com.hazelcast.map.impl.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
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

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class QueryRunnerTest extends HazelcastTestSupport {

    private HazelcastInstance instance;
    private IMap<String, String> map;
    private QueryRunner queryRunner;

    private int partitionId;
    private String key;
    private String value;


    @Before
    public void before() {
        instance = createHazelcastInstance();
        map = instance.getMap(randomName());
        queryRunner = getQueryRunner();

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
    public void assertSequentialQueryRunner() {
        assertEquals(QueryRunner.class, getQueryRunner().getClass());
    }

    @Test
    public void runFullQuery() throws ExecutionException, InterruptedException {
        Predicate predicate = Predicates.equal("this", value);
        Query query = Query.of().mapName(map.getName()).predicate(predicate).iterationType(IterationType.ENTRY).build();
        QueryResult result = (QueryResult) queryRunner.run(query);

        assertEquals(1, result.getRows().size());
        assertEquals(map.get(key), toObject(result.getRows().iterator().next().getValue()));
    }

    @Test
    public void runPartitionScanQueryOnSinglePartition() throws ExecutionException, InterruptedException {
        Predicate predicate = Predicates.equal("this", value);
        Query query = Query.of().mapName(map.getName()).predicate(predicate).iterationType(IterationType.ENTRY).build();
        QueryResult result = (QueryResult) queryRunner.runUsingPartitionScanOnSinglePartition(query, partitionId);

        assertEquals(1, result.getRows().size());
        assertEquals(map.get(key), toObject(result.getRows().iterator().next().getValue()));
    }

    private QueryRunner getQueryRunner() {
        MapService mapService = getNodeEngineImpl(instance).getService(MapService.SERVICE_NAME);
        return mapService.getMapServiceContext().getMapQueryRunner("");
    }

    private Object toObject(Data data) {
        return getSerializationService(instance).toObject(data);
    }

}
