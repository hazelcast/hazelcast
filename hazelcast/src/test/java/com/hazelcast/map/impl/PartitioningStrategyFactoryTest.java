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
import org.hamcrest.core.IsSame;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import static com.hazelcast.map.impl.PartitioningStrategyFactory.getPartitioningStrategy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PartitioningStrategyFactoryTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testUtilityClassWithPrivateConstructor() {
        assertUtilityConstructor(PartitioningStrategyFactory.class);
    }

    @Test
    public void whenConfigNull_getPartitioningStrategy_returnsNull() {
        HazelcastInstance hz = createHazelcastInstance();
        NodeEngine nodeEngine = getNodeEngineImpl(hz);
        PartitioningStrategy partitioningStrategy = getPartitioningStrategy(nodeEngine, "map", null);
        assertNull(partitioningStrategy);
    }

    @Test
    public void whenPartitioningStrategyDefined_getPartitioningStrategy_returnsSameInstance() {
        HazelcastInstance hz = createHazelcastInstance();
        NodeEngine nodeEngine = getNodeEngineImpl(hz);
        PartitioningStrategy configuredPartitioningStrategy = new StringPartitioningStrategy();
        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig(configuredPartitioningStrategy);
        PartitioningStrategy partitioningStrategy = getPartitioningStrategy(nodeEngine, "map", cfg);
        assertSame(configuredPartitioningStrategy, partitioningStrategy);
    }

    @Test
    public void whenPartitioningStrategyClassDefined_getPartitioningStrategy_returnsNewInstance() {
        HazelcastInstance hz = createHazelcastInstance();
        NodeEngine nodeEngine = getNodeEngineImpl(hz);
        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig();
        cfg.setPartitioningStrategyClass("com.hazelcast.partition.strategy.StringPartitioningStrategy");
        PartitioningStrategy partitioningStrategy = getPartitioningStrategy(nodeEngine, "map", cfg);
        assertEquals(StringPartitioningStrategy.class, partitioningStrategy.getClass());
    }

    // when a partitioning strategy has already been cached, then another invocation to obtain the partitioning
    // strategy for the same map name retrieves the same instance
    @Test
    public void whenStrategyForMapAlreadyDefined_getPartitioningStrategy_returnsSameInstance() {
        HazelcastInstance hz = createHazelcastInstance();
        NodeEngine nodeEngine = getNodeEngineImpl(hz);
        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig();
        cfg.setPartitioningStrategyClass("com.hazelcast.partition.strategy.StringPartitioningStrategy");
        // when we have already obtained the partitioning strategy for a given map name
        PartitioningStrategy partitioningStrategy = getPartitioningStrategy(nodeEngine, "map", cfg);
        PartitioningStrategy cachedPartitioningStrategy = getPartitioningStrategy(nodeEngine, "map", cfg);
        assertSame(partitioningStrategy, cachedPartitioningStrategy);
    }

    // when an exception is thrown while attempting to instantiate a partitioning strategy
    // then the exception is rethrown (the same if it is a RuntimeException, otherwise it is peeled,
    // see ExceptionUtil.rethrow for all the details).
    @Test
    public void whenStrategyInstantiationThrowsException_getSamePartitioningStrategy_rethrowsException() {
        NodeEngine nodeEngine = mock(NodeEngine.class);
        RuntimeException e = new RuntimeException("expected exception");
        Mockito.when(nodeEngine.getConfigClassLoader()).thenThrow(e);

        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig();
        cfg.setPartitioningStrategyClass("com.hazelcast.partition.strategy.StringPartitioningStrategy");

        // while attempting to get partitioning strategy, exception will be thrown
        expectedException.expect(new IsSame<RuntimeException>(e));
        getPartitioningStrategy(nodeEngine,
                "whenStrategyInstantiationThrowsException_getSamePartitioningStrategy_rethrowsException", cfg);
    }
}
