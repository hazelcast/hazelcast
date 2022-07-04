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
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.util.IterationType.ENTRY;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class QueryEngineImpl_queryLocalPartition_resultSizeLimitTest extends HazelcastTestSupport {

    private static final int RESULT_SIZE_LIMIT = QueryResultSizeLimiter.MINIMUM_MAX_RESULT_LIMIT + 2342;
    private static final int PARTITION_COUNT = 2;
    private static final int PARTITION_ID = 0;

    private HazelcastInstance hz;
    private IMap<String, String> map;
    private QueryEngineImpl queryEngine;
    private int limit;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(QueryEngineImpl.DISABLE_MIGRATION_FALLBACK.getName(), "true");
        // we reduce the number of partitions to speed up content generation
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "" + PARTITION_COUNT);
        config.setProperty(ClusterProperty.QUERY_RESULT_SIZE_LIMIT.getName(), "" + RESULT_SIZE_LIMIT);

        hz = createHazelcastInstance(config);
        map = hz.getMap(randomName());

        MapService mapService = getNodeEngineImpl(hz).getService(MapService.SERVICE_NAME);
        queryEngine = new QueryEngineImpl(mapService.getMapServiceContext());

        // we just fill a single partition, so we get the NodeResultLimit for a single partition as well
        QueryResultSizeLimiter resultSizeLimiter = queryEngine.getQueryResultSizeLimiter();
        limit = (int) resultSizeLimiter.getNodeResultLimit(1);
    }

    @Test
    public void checkResultSize_limitNotExceeded() {
        fillPartition(limit - 1);

        Query query = Query.of().mapName(map.getName()).predicate(Predicates.alwaysTrue()).iterationType(ENTRY).build();
        QueryResult result = (QueryResult) queryEngine.execute(query, Target.LOCAL_NODE);

        assertEquals(limit - 1, result.size());
    }

    @Test
    public void checkResultSize_limitNotEquals() {
        fillPartition(limit);

        Query query = Query.of().mapName(map.getName()).predicate(Predicates.alwaysTrue()).iterationType(ENTRY).build();
        QueryResult result = (QueryResult) queryEngine.execute(query, Target.LOCAL_NODE);

        assertEquals(limit, result.size());
    }

    @Ignore
    @Test(expected = QueryResultSizeExceededException.class)
    public void checkResultSize_limitExceeded() {
        fillPartition(limit + 1);

        Query query = Query.of().mapName(map.getName()).predicate(Predicates.alwaysTrue()).iterationType(ENTRY).build();
        queryEngine.execute(query, Target.LOCAL_NODE);
    }

    private void fillPartition(int count) {
        for (int i = 0; i < count; i++) {
            String key = generateKeyForPartition(hz, PARTITION_ID);
            map.put(key, "");
        }
    }
}
