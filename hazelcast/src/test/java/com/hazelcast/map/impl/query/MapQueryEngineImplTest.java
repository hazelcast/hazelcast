/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.util.IterationType.ENTRY;
import static com.hazelcast.util.IterationType.KEY;
import static com.hazelcast.util.IterationType.VALUE;
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
        Predicate predicate = Predicates.equal("this", value);
        Query query = Query.of().mapName(map.getName()).predicate(predicate).iterationType(KEY).build();

        QueryResult result = queryEngine.execute(query, Target.ALL_NODES);

        assertEquals(1, result.size());
        assertEquals(key, toObject(result.iterator().next().getKey()));
    }

    @Test
    public void runQueryOnLocalPartitions() throws ExecutionException, InterruptedException {
        Predicate predicate = Predicates.equal("this", value);
        Query query = Query.of().mapName(map.getName()).predicate(predicate).iterationType(KEY).build();

        QueryResult result = queryEngine.execute(query, Target.LOCAL_NODE);

        assertEquals(1, result.size());
        assertEquals(key, toObject(result.iterator().next().getKey()));
    }

    @Test
    public void runQueryOnAllPartitions_key() throws ExecutionException, InterruptedException {
        Predicate predicate = Predicates.equal("this", value);
        Query query = Query.of().mapName(map.getName()).predicate(predicate).iterationType(KEY).build();

        QueryResult result = queryEngine.execute(query, Target.ALL_NODES);

        assertEquals(1, result.size());
        assertEquals(key, toObject(result.iterator().next().getKey()));
    }

    @Test
    public void runQueryOnAllPartitions_value() throws ExecutionException, InterruptedException {
        Predicate predicate = Predicates.equal("this", value);
        Query query = Query.of().mapName(map.getName()).predicate(predicate).iterationType(VALUE).build();

        QueryResult result = queryEngine.execute(query, Target.ALL_NODES);

        assertEquals(1, result.size());
        assertEquals(value, toObject(result.iterator().next().getValue()));
    }

    @Test
    public void runQueryOnGivenPartition() throws ExecutionException, InterruptedException {
        Predicate predicate = Predicates.equal("this", value);
        Query query = Query.of().mapName(map.getName()).predicate(predicate).iterationType(ENTRY).build();

        QueryResult result = queryEngine.execute(query, Target.of().partitionOwner(partitionId).build());

        assertEquals(1, result.size());
        assertEquals(key, toObject(((Map.Entry) result.iterator().next()).getKey()));
        assertEquals(map.get(key), toObject(((Map.Entry) result.iterator().next()).getValue()));
    }

    private Object toObject(Object data) {
        return getSerializationService(instance).toObject(data);
    }

}
