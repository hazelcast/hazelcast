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
import com.hazelcast.map.impl.MapService;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.util.IterationType.ENTRY;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryEngineImpl_queryLocalPartitions_resultSizeNoLimitTest extends HazelcastTestSupport {

    private IMap<Object, Object> map;
    private QueryEngineImpl queryEngine;

    @Before
    public void setup() {
        Config config = regularInstanceConfig()
                .setProperty(QueryEngineImpl.DISABLE_MIGRATION_FALLBACK.getName(), "true");
        HazelcastInstance hz = createHazelcastInstance(config);
        map = hz.getMap(randomName());

        MapService mapService = getNodeEngineImpl(hz).getService(MapService.SERVICE_NAME);
        queryEngine = new QueryEngineImpl(mapService.getMapServiceContext());
    }

    @Test
    public void checkResultSize() {
        fillMap(10000);

        Query query = Query.of().mapName(map.getName()).predicate(Predicates.alwaysTrue()).iterationType(ENTRY).build();
        QueryResult result = (QueryResult) queryEngine.execute(query, Target.LOCAL_NODE);

        assertEquals(10000, result.size());
    }

    private void fillMap(long count) {
        for (long i = 0; i < count; i++) {
            map.put(i + randomString(), "");
        }
    }
}
