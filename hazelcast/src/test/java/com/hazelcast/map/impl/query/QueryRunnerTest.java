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
    public void runFullQuery() {
        Predicate predicate = Predicates.equal("this", value);
        Query query = Query.of().mapName(map.getName()).predicate(predicate).iterationType(IterationType.ENTRY).build();
        QueryResult result = (QueryResult) queryRunner.runIndexOrPartitionScanQueryOnOwnedPartitions(query);

        assertEquals(1, result.getRows().size());
        assertEquals(map.get(key), toObject(result.getRows().iterator().next().getValue()));
    }

    @Test
    public void runPartitionScanQueryOnSinglePartition() {
        Predicate predicate = Predicates.equal("this", value);
        Query query = Query.of().mapName(map.getName()).predicate(predicate).iterationType(IterationType.ENTRY).build();
        QueryResult result = (QueryResult) queryRunner.runPartitionScanQueryOnGivenOwnedPartition(query, partitionId);

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
