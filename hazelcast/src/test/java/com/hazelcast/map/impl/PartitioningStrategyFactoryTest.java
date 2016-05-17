/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

/**
 *
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PartitioningStrategyFactoryTest extends HazelcastTestSupport {

    @Test
    public void getPartitioningStrategy_whenConfigNull()
            throws Exception {
        HazelcastInstance hz = createHazelcastInstance();
        NodeEngine nodeEngine = getNodeEngineImpl(hz);
        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.getPartitioningStrategy(nodeEngine, "map", null);
        assertNull(partitioningStrategy);
    }

    @Test
    public void getPartitioningStrategy_whenPartitioningStrategyDefined()
            throws Exception {
        HazelcastInstance hz = createHazelcastInstance();
        NodeEngine nodeEngine = getNodeEngineImpl(hz);
        PartitioningStrategy configuredPartitioningStrategy = new StringPartitioningStrategy();
        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig(configuredPartitioningStrategy);
        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.getPartitioningStrategy(nodeEngine, "map", cfg);
        assertSame(configuredPartitioningStrategy, partitioningStrategy);
    }

    @Test
    public void getPartitioningStrategy_whenPartitioningStrategyClassDefined()
            throws Exception {
        HazelcastInstance hz = createHazelcastInstance();
        NodeEngine nodeEngine = getNodeEngineImpl(hz);
        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig();
        cfg.setPartitioningStrategyClass("com.hazelcast.partition.strategy.StringPartitioningStrategy");
        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.getPartitioningStrategy(nodeEngine, "map", cfg);
        assertEquals(StringPartitioningStrategy.class, partitioningStrategy.getClass());
    }

}