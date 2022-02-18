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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.query.Target.TargetMode;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.FutureUtil.returnWithDeadline;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getSerializationService;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class QueryEngine_DispatchTest extends HazelcastTestSupport {

    private HazelcastInstance instance;
    private IMap<String, String> map;
    private QueryEngineImpl queryEngine;

    private int partitionId;
    private String key;
    private String value;

    @Before
    public void before() {
        Config config = regularInstanceConfig()
                .setProperty(QueryEngineImpl.DISABLE_MIGRATION_FALLBACK.getName(), "true");
        instance = createHazelcastInstance(config);
        map = instance.getMap(randomName());
        MapService mapService = getNodeEngineImpl(instance).getService(MapService.SERVICE_NAME);
        queryEngine = new QueryEngineImpl(mapService.getMapServiceContext());

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
    public void dispatchFullQueryOnQueryThread_localMembers() {
        dispatchFullQueryOnQueryThread(TargetMode.LOCAL_NODE);
    }

    @Test
    public void dispatchFullQueryOnQueryThread_allMembers() {
        dispatchFullQueryOnQueryThread(TargetMode.ALL_NODES);
    }

    private void dispatchFullQueryOnQueryThread(TargetMode target) {
        Query query = Query.of().mapName(map.getName()).predicate(Predicates.equal("this", value))
                .partitionIdSet(queryEngine.getAllPartitionIds())
                .iterationType(IterationType.ENTRY).build();
        List<Future<Result>> futures = queryEngine
                .dispatchFullQueryOnQueryThread(query, target);
        Collection<Result> results = returnWithDeadline(futures, 1, TimeUnit.MINUTES);
        QueryResult result = (QueryResult) results.iterator().next();

        assertEquals(1, results.size());
        assertEquals(1, result.size());
        assertEquals(map.get(key), toObject(result.getRows().iterator().next().getValue()));
    }

    @Test
    public void dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread_singlePartition() {
        Query query = Query.of().mapName(map.getName()).predicate(Predicates.equal("this", value))
                .iterationType(IterationType.ENTRY).build();
        Future<Result> future = queryEngine
                .dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(query, partitionId);
        Collection<Result> results = returnWithDeadline(Collections.singletonList(future), 1, TimeUnit.MINUTES);
        QueryResult result = (QueryResult) results.iterator().next();

        assertEquals(1, results.size());
        assertEquals(1, result.size());
        assertEquals(map.get(key), toObject(result.getRows().iterator().next().getValue()));
    }

    private Object toObject(Data data) {
        return getSerializationService(instance).toObject(data);
    }

}
