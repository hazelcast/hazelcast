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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.util.IterationType.ENTRY;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelTest.class})
public class MapQueryEngineImpl_queryLocalPartitions_resultSizeLimitTest extends HazelcastTestSupport {

    private static final long RESULT_SIZE_LIMIT = QueryResultSizeLimiter.MINIMUM_MAX_RESULT_LIMIT + 4223;
    private static final int PARTITION_COUNT = 271;

    private IMap<String, String> map;
    private MapQueryEngineImpl queryEngine;
    private int limit;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "" + PARTITION_COUNT);
        config.setProperty(GroupProperty.QUERY_RESULT_SIZE_LIMIT.getName(), "" + RESULT_SIZE_LIMIT);

        HazelcastInstance hz = createHazelcastInstance(config);
        map = hz.getMap(randomName());

        MapService mapService = getNodeEngineImpl(hz).getService(MapService.SERVICE_NAME);
        queryEngine = new MapQueryEngineImpl(mapService.getMapServiceContext());

        // we fill all partitions, so we get the NodeResultLimit for all partitions as well
        QueryResultSizeLimiter resultSizeLimiter = queryEngine.getQueryResultSizeLimiter();
        limit = (int) resultSizeLimiter.getNodeResultLimit(PARTITION_COUNT);
    }

    @Test
    public void checkResultSize_limitNotExceeded() throws Exception {
        fillMap(limit - 1);

        Query query = Query.of().mapName(map.getName()).predicate(TruePredicate.INSTANCE)
                .iterationType(ENTRY).build();
        QueryResult result = (QueryResult) queryEngine.execute(query, Target.LOCAL_NODE);

        assertEquals(limit - 1, result.size());
    }

    @Test
    public void checkResultSize_limitEquals() throws Exception {
        fillMap(limit);

        Query query = Query.of().mapName(map.getName()).predicate(TruePredicate.INSTANCE)
                .iterationType(ENTRY).build();
        QueryResult result = (QueryResult) queryEngine.execute(query, Target.LOCAL_NODE);

        assertEquals(limit, result.size());
    }

    @Test(expected = QueryResultSizeExceededException.class)
    public void checkResultSize_limitExceeded() throws Exception {
        fillMap(limit + 1);

        Query query = Query.of().mapName(map.getName()).predicate(TruePredicate.INSTANCE)
                .iterationType(ENTRY).build();
        queryEngine.execute(query, Target.LOCAL_NODE);
    }

    private void fillMap(long count) {
        for (long i = 0; i < count; i++) {
            map.put(i + randomString(), "");
        }
    }
}
