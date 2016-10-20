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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.query.MapQueryDispatcher.DispatchTarget.ALL_MEMBERS;
import static com.hazelcast.map.impl.query.MapQueryDispatcher.DispatchTarget.LOCAL_MEMBER;
import static com.hazelcast.util.FutureUtil.returnWithDeadline;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class MapQueryDispatcherTest extends HazelcastTestSupport {

    private HazelcastInstance instance;
    private IMap<String, String> map;
    private MapQueryDispatcher queryDispatcher;

    private int partitionId;
    private String key;
    private String value;

    @Before
    public void before() {
        instance = createHazelcastInstance();
        map = instance.getMap(randomName());
        MapService mapService = getNodeEngineImpl(instance).getService(MapService.SERVICE_NAME);
        queryDispatcher = new MapQueryDispatcher(mapService.getMapServiceContext());

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
    public void dispatchFullQueryOnQueryThread_localMembers() throws ExecutionException, InterruptedException {
        dispatchFullQueryOnQueryThread(LOCAL_MEMBER);
    }

    @Test
    public void dispatchFullQueryOnQueryThread_allMembers() throws ExecutionException, InterruptedException {
        dispatchFullQueryOnQueryThread(ALL_MEMBERS);
    }

    private void dispatchFullQueryOnQueryThread(MapQueryDispatcher.DispatchTarget target) {
        List<Future<QueryResult>> futures = queryDispatcher
                .dispatchFullQueryOnQueryThread(map.getName(), Predicates.equal("this", value), IterationType.ENTRY, target);
        Collection<QueryResult> results = returnWithDeadline(futures, 1, TimeUnit.MINUTES);
        QueryResult result = results.iterator().next();

        assertEquals(1, results.size());
        assertEquals(1, result.size());
        assertEquals(map.get(key), toObject(result.getRows().iterator().next().getValue()));
    }

    @Test
    public void dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread_singlePartition() throws ExecutionException, InterruptedException {
        Predicate predicate = Predicates.equal("this", value);
        Future<QueryResult> future = queryDispatcher
                .dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(map.getName(), predicate, partitionId, IterationType.ENTRY);
        Collection<QueryResult> results = returnWithDeadline(Collections.singletonList(future), 1, TimeUnit.MINUTES);
        QueryResult result = results.iterator().next();

        assertEquals(1, results.size());
        assertEquals(1, result.size());
        assertEquals(map.get(key), toObject(result.getRows().iterator().next().getValue()));
    }

    @Test
    public void dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread_multiplePartitions() throws ExecutionException, InterruptedException {
        Predicate predicate = Predicates.equal("this", value);
        List<Future<QueryResult>> futures = queryDispatcher
                .dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(map.getName(), predicate, asList(partitionId), IterationType.ENTRY);
        ArrayList<QueryResult> results = new ArrayList<QueryResult>(returnWithDeadline(futures, 1, TimeUnit.MINUTES));
        QueryResult result = results.iterator().next();

        assertEquals(1, results.size());
        assertEquals(1, result.size());
        assertEquals(map.get(key), toObject(result.getRows().iterator().next().getValue()));
    }

    private Object toObject(Data data) {
        return getSerializationService(instance).toObject(data);
    }

}
