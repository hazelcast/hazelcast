/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.internal.util.SetUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;
import java.util.UUID;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getSerializationService;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class QueryRunnerTest extends HazelcastTestSupport {

    private HazelcastInstance instance;
    private IMap<String, String> map;
    private QueryRunner queryRunner;
    private MapService mapService;

    private int partitionId;
    private String key;
    private String value;

    @Before
    public void before() {
        instance = createHazelcastInstance();
        mapService = getMapService();
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
        assertEquals(QueryRunner.class, queryRunner.getClass());
    }

    @Test
    public void runFullQuery() {
        Predicate predicate = Predicates.equal("this", value);
        Query query = Query.of()
                .mapName(map.getName())
                .predicate(predicate)
                .iterationType(IterationType.ENTRY)
                .partitionIdSet(SetUtil.allPartitionIds(instance.getPartitionService().getPartitions().size()))
                .build();
        QueryResult result = (QueryResult) queryRunner.runIndexOrPartitionScanQueryOnOwnedPartitions(query);

        assertEquals(1, result.getRows().size());
        assertEquals(map.get(key), toObject(result.getRows().iterator().next().getValue()));
        assertArrayEquals(result.getPartitionIds().toArray(), mapService.getMapServiceContext().getOrInitCachedMemberPartitions().toArray());
    }

    @Test
    public void verifyIndexedQueryFailureWhileMigrating() {
        map.addIndex(IndexType.HASH, "this");
        Predicate predicate = new EqualPredicate("this", value);

        mapService.beforeMigration(new PartitionMigrationEvent(MigrationEndpoint.SOURCE, partitionId, 0, 1, UUID.randomUUID()));

        Query query = Query.of()
                .mapName(map.getName())
                .predicate(predicate)
                .iterationType(IterationType.ENTRY)
                .partitionIdSet(SetUtil.allPartitionIds(instance.getPartitionService().getPartitions().size()))
                .build();
        QueryResult result = (QueryResult) queryRunner.runIndexOrPartitionScanQueryOnOwnedPartitions(query);
        assertNull(result.getPartitionIds());
    }

    @Test
    public void verifyIndexedQueryFailureWhileMigratingInFlight() {
        map.addIndex(IndexType.HASH, "this");

        Predicate predicate = new EqualPredicate("this", value) {
            @Override
            public Set<QueryableEntry> filter(QueryContext queryContext) {
                // start a new migration while executing an indexed query
                mapService.beforeMigration(new PartitionMigrationEvent(MigrationEndpoint.SOURCE, partitionId, 0, 1,
                        UUID.randomUUID()));
                return super.filter(queryContext);
            }
        };
        Query query = Query.of()
                .mapName(map.getName())
                .predicate(predicate)
                .iterationType(IterationType.ENTRY)
                .partitionIdSet(SetUtil.allPartitionIds(instance.getPartitionService().getPartitions().size()))
                .build();
        QueryResult result = (QueryResult) queryRunner.runIndexOrPartitionScanQueryOnOwnedPartitions(query);
        assertNull(result.getPartitionIds());
    }

    @Test
    public void verifyFullScanFailureWhileMigrating() {
        Predicate predicate = new EqualPredicate("this", value);

        mapService.beforeMigration(new PartitionMigrationEvent(MigrationEndpoint.SOURCE, partitionId, 0, 1, UUID.randomUUID()));

        Query query = Query.of()
                .mapName(map.getName())
                .predicate(predicate)
                .iterationType(IterationType.ENTRY)
                .partitionIdSet(SetUtil.allPartitionIds(instance.getPartitionService().getPartitions().size()))
                .build();
        QueryResult result = (QueryResult) queryRunner.runIndexOrPartitionScanQueryOnOwnedPartitions(query);
        assertNull(result.getPartitionIds());
    }

    @Test
    public void verifyFullScanFailureWhileMigratingInFlight() {
        Predicate predicate = new EqualPredicate("this", value) {
            @Override
            protected boolean applyForSingleAttributeValue(Comparable attributeValue) {
                // start a new migration while executing a full scan
                mapService.beforeMigration(new PartitionMigrationEvent(MigrationEndpoint.SOURCE, partitionId, 0, 1,
                        UUID.randomUUID()));
                return super.applyForSingleAttributeValue(attributeValue);
            }
        };
        Query query = Query.of()
                .mapName(map.getName())
                .predicate(predicate)
                .iterationType(IterationType.ENTRY)
                .partitionIdSet(SetUtil.allPartitionIds(instance.getPartitionService().getPartitions().size()))
                .build();
        QueryResult result = (QueryResult) queryRunner.runIndexOrPartitionScanQueryOnOwnedPartitions(query);
        assertNull(result.getPartitionIds());
    }

    private MapService getMapService() {
        return getNodeEngineImpl(instance).getService(MapService.SERVICE_NAME);
    }

    private QueryRunner getQueryRunner() {
        return mapService.getMapServiceContext().getMapQueryRunner(map.getName());
    }

    private Object toObject(Data data) {
        return getSerializationService(instance).toObject(data);
    }

}
