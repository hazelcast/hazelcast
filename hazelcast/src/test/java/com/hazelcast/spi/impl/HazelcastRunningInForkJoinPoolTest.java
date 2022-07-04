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

package com.hazelcast.spi.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastRunningInForkJoinPoolTest extends HazelcastTestSupport {

    protected static final String MAP_NAME = "near-cached-map";

    protected HazelcastInstance hz;

    @Before
    public void setUp() throws Exception {
        Config config = getConfig();

        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        mapConfig.setNearCacheConfig(getNearCacheConfig());

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        hz = factory.newHazelcastInstance(config);
        hz = factory.newHazelcastInstance(config);
    }

    protected static NearCacheConfig getNearCacheConfig() {
        return new NearCacheConfig(MAP_NAME)
                .setInvalidateOnChange(true);
    }

    /**
     * Both Java's parallel-stream-api and Hazelcast use
     * {@link ForkJoinPool#commonPool()} for their internal
     * mechanisms to run. We are testing whether or not
     * there is a deadlock during proxy initialization. With
     * near cache enabled, iMap tries to attach listeners at
     * proxy init phase and this triggers deadlock situation.
     */
    @Test
    public void no_deadlock_during_proxy_initialization() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        int count = availableProcessors + 1;
        List<IMap<Object, Object>> maps = IntStream.range(0, count)
                .boxed()
                .parallel()
                // init proxy in parallel
                .map(i -> hz.getMap(MAP_NAME))
                .collect(Collectors.toList());

        assertEquals(count, maps.size());
    }
}
