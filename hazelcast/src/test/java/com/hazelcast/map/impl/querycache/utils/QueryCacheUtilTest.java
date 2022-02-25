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

package com.hazelcast.map.impl.querycache.utils;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.querycache.NodeQueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.map.impl.querycache.utils.QueryCacheUtil.getAccumulatorOrNull;
import static com.hazelcast.map.impl.querycache.utils.QueryCacheUtil.getAccumulators;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheUtilTest extends HazelcastTestSupport {

    private QueryCacheContext context;

    @Before
    public void setUp() {
        HazelcastInstance instance = createHazelcastInstance();
        MapService mapService = getNodeEngineImpl(instance).getService(MapService.SERVICE_NAME);

        context = new NodeQueryCacheContext(mapService.getMapServiceContext());
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(QueryCacheUtil.class);
    }

    @Test
    public void getAccumulators_whenNoAccumulatorsRegistered_thenReturnEmptyMap() {
        Map<Integer, Accumulator> accumulators = getAccumulators(context, "myMap", "myCache");

        assertNotNull(accumulators);
        assertEquals(0, accumulators.size());
    }

    @Test
    public void getAccumulatorOrNull_whenNoAccumulatorsRegistered_thenReturnNull() {
        Accumulator accumulator = getAccumulatorOrNull(context, "myMap", "myCache", -1);

        assertNull(accumulator);
    }
}
