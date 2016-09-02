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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.core.IsSame;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.UUID;

import static com.hazelcast.map.impl.PartitioningStrategyFactory.getPartitioningStrategy;
import static com.hazelcast.map.impl.PartitioningStrategyFactory.key;
import static com.hazelcast.map.impl.PartitioningStrategyFactory.removePartitioningStrategyFromCache;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PartitioningStrategyFactoryTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TestName testName = new TestName();
    
    String mapName;
    
    @Before
    public void setup() {
        mapName = testName.getMethodName();
    }

    @Test
    public void testUtilityClassWithPrivateConstructor() {
        assertUtilityConstructor(PartitioningStrategyFactory.class);
    }

    @Test
    public void whenConfigNull_getPartitioningStrategy_returnsNull() {
        HazelcastInstance hz = createHazelcastInstance();
        NodeEngine nodeEngine = getNodeEngineImpl(hz);
        PartitioningStrategy partitioningStrategy = getPartitioningStrategy(nodeEngine, mapName, null);
        assertNull(partitioningStrategy);
    }

    @Test
    public void whenPartitioningStrategyDefined_getPartitioningStrategy_returnsSameInstance() {
        HazelcastInstance hz = createHazelcastInstance();
        NodeEngine nodeEngine = getNodeEngineImpl(hz);
        PartitioningStrategy configuredPartitioningStrategy = new StringPartitioningStrategy();
        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig(configuredPartitioningStrategy);
        PartitioningStrategy partitioningStrategy = getPartitioningStrategy(nodeEngine, mapName, cfg);
        assertSame(configuredPartitioningStrategy, partitioningStrategy);
    }

    @Test
    public void whenPartitioningStrategyClassDefined_getPartitioningStrategy_returnsNewInstance() {
        HazelcastInstance hz = createHazelcastInstance();
        NodeEngine nodeEngine = getNodeEngineImpl(hz);
        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig();
        cfg.setPartitioningStrategyClass("com.hazelcast.partition.strategy.StringPartitioningStrategy");
        PartitioningStrategy partitioningStrategy = getPartitioningStrategy(nodeEngine, mapName, cfg);
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
        PartitioningStrategy partitioningStrategy = getPartitioningStrategy(nodeEngine, mapName, cfg);
        PartitioningStrategy cachedPartitioningStrategy = getPartitioningStrategy(nodeEngine, mapName, cfg);
        assertSame(partitioningStrategy, cachedPartitioningStrategy);
    }

    // when an exception is thrown while attempting to instantiate a partitioning strategy
    // then the exception is rethrown (the same if it is a RuntimeException, otherwise it is peeled,
    // see ExceptionUtil.rethrow for all the details).
    @Test
    public void whenStrategyInstantiationThrowsException_getSamePartitioningStrategy_rethrowsException() {
        Member localMember = mock(Member.class);
        when(localMember.getUuid()).thenReturn(UUID.randomUUID().toString());

        NodeEngine nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getLocalMember()).thenReturn(localMember);

        RuntimeException e = new RuntimeException("expected exception");
        when(nodeEngine.getConfigClassLoader()).thenThrow(e);

        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig();
        cfg.setPartitioningStrategyClass("com.hazelcast.partition.strategy.StringPartitioningStrategy");

        // while attempting to get partitioning strategy, exception will be thrown
        expectedException.expect(new IsSame<RuntimeException>(e));
        getPartitioningStrategy(nodeEngine,
                "whenStrategyInstantiationThrowsException_getSamePartitioningStrategy_rethrowsException", cfg);
    }

    @Test
    public void whenRemoveInvoked_entryIsRemovedFromCache() {
        HazelcastInstance hz = createHazelcastInstance();
        NodeEngine nodeEngine = getNodeEngineImpl(hz);
        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig();
        cfg.setPartitioningStrategyClass("com.hazelcast.partition.strategy.StringPartitioningStrategy");
        PartitioningStrategy partitioningStrategy = getPartitioningStrategy(nodeEngine, mapName, cfg);
        removePartitioningStrategyFromCache(nodeEngine, mapName);
        assertFalse(PartitioningStrategyFactory.CACHE.containsKey(key(nodeEngine, mapName)));
    }

    @Test
    public void whenMapDestroyed_entryIsRemovedFromCache() {
        Config config = new Config();
        MapConfig mapConfig = new MapConfig(mapName);
        PartitioningStrategyConfig partitioningStrategyConfig = new PartitioningStrategyConfig();
        partitioningStrategyConfig.setPartitioningStrategyClass("com.hazelcast.partition.strategy.StringPartitioningStrategy");
        mapConfig.setPartitioningStrategyConfig(partitioningStrategyConfig);
        config.addMapConfig(mapConfig);

        HazelcastInstance hz = createHazelcastInstance(config);
        NodeEngine nodeEngine = getNodeEngineImpl(hz);

        IMap<String, String> map = hz.getMap(mapName);
        map.put("a", "b");
        // at this point, the PartitioningStrategyFactory#CACHE should have an entry for this map config
        final String internalCacheKey = key(nodeEngine, mapName);
        assertTrue("Key should exist in cache", PartitioningStrategyFactory.CACHE.containsKey(internalCacheKey));

        map.destroy();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertFalse("Key should have been removed from cache",
                        PartitioningStrategyFactory.CACHE.containsKey(internalCacheKey));
            }
        }, 20);
    }
}
