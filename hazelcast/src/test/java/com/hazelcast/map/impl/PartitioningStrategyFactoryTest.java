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

package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.RootCauseMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitioningStrategyFactoryTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TestName testName = new TestName();

    private IMap<String, String> map;
    private String mapName;
    private PartitioningStrategyFactory partitioningStrategyFactory;

    @Before
    public void setup() {
        mapName = testName.getMethodName();
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        map = hazelcastInstance.getMap(mapName);
        MapProxyImpl mapProxy = (MapProxyImpl) map;
        MapService mapService = (MapService) mapProxy.getService();
        MapServiceContextImpl mapServiceContext = (MapServiceContextImpl) mapService.getMapServiceContext();
        partitioningStrategyFactory = mapServiceContext.getPartitioningStrategyFactory();
    }

    @Test
    public void whenConfigNull_getPartitioningStrategy_returnsNull() {
        PartitioningStrategy partitioningStrategy = partitioningStrategyFactory.getPartitioningStrategy(mapName, null);
        assertNull(partitioningStrategy);
    }

    @Test
    public void whenPartitioningStrategyDefined_getPartitioningStrategy_returnsSameInstance() {
        PartitioningStrategy configuredPartitioningStrategy = new StringPartitioningStrategy();
        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig(configuredPartitioningStrategy);
        PartitioningStrategy partitioningStrategy = partitioningStrategyFactory.getPartitioningStrategy(mapName, cfg);
        assertSame(configuredPartitioningStrategy, partitioningStrategy);
    }

    @Test
    public void whenPartitioningStrategyClassDefined_getPartitioningStrategy_returnsNewInstance() {
        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig();
        cfg.setPartitioningStrategyClass("com.hazelcast.partition.strategy.StringPartitioningStrategy");
        PartitioningStrategy partitioningStrategy = partitioningStrategyFactory.getPartitioningStrategy(mapName, cfg);
        assertEquals(StringPartitioningStrategy.class, partitioningStrategy.getClass());
    }

    // when a partitioning strategy has already been cached, then another invocation to obtain the partitioning
    // strategy for the same map name retrieves the same instance
    @Test
    public void whenStrategyForMapAlreadyDefined_getPartitioningStrategy_returnsSameInstance() {
        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig();
        cfg.setPartitioningStrategyClass("com.hazelcast.partition.strategy.StringPartitioningStrategy");
        // when we have already obtained the partitioning strategy for a given map name
        PartitioningStrategy partitioningStrategy = partitioningStrategyFactory.getPartitioningStrategy(mapName, cfg);
        // then once we get it again with the same arguments, we retrieve the same instance
        PartitioningStrategy cachedPartitioningStrategy = partitioningStrategyFactory.getPartitioningStrategy(mapName, cfg);
        assertSame(partitioningStrategy, cachedPartitioningStrategy);
    }

    // when an exception is thrown while attempting to instantiate a partitioning strategy
    // then the exception is rethrown (the same if it is a RuntimeException, otherwise it is peeled,
    // see ExceptionUtil.rethrow for all the details).
    @Test
    public void whenStrategyInstantiationThrowsException_getPartitioningStrategy_rethrowsException() {
        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig();
        cfg.setPartitioningStrategyClass("NonExistentPartitioningStrategy");

        // while attempting to get partitioning strategy, ClassNotFound exception will be thrown and wrapped in HazelcastException
        expectedException.expect(new RootCauseMatcher(ClassNotFoundException.class));
        // use a random UUID as map name, to avoid obtaining the PartitioningStrategy from cache.
        partitioningStrategyFactory.getPartitioningStrategy(UUID.randomUUID().toString(), cfg);
    }

    @Test
    public void whenRemoveInvoked_entryIsRemovedFromCache() {
        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig();
        cfg.setPartitioningStrategyClass("com.hazelcast.partition.strategy.StringPartitioningStrategy");
        PartitioningStrategy partitioningStrategy = partitioningStrategyFactory.getPartitioningStrategy(mapName, cfg);
        partitioningStrategyFactory.removePartitioningStrategyFromCache(mapName);
        assertFalse(partitioningStrategyFactory.cache.containsKey(mapName));
    }

    @Test
    public void whenMapDestroyed_entryIsRemovedFromCache() {
        map.put("a", "b");
        // at this point, the PartitioningStrategyFactory#cache should have an entry for this map config
        assertTrue("Key should exist in cache", partitioningStrategyFactory.cache.containsKey(mapName));

        map.destroy();

        assertTrueEventually(() -> assertFalse("Key should have been removed from cache",
                partitioningStrategyFactory.cache.containsKey(mapName)), 20);
    }

    @Override
    protected Config getConfig() {
        Config config = new Config();
        MapConfig mapConfig = new MapConfig(mapName);
        PartitioningStrategyConfig partitioningStrategyConfig = new PartitioningStrategyConfig();
        partitioningStrategyConfig.setPartitioningStrategyClass("com.hazelcast.partition.strategy.StringPartitioningStrategy");
        mapConfig.setPartitioningStrategyConfig(partitioningStrategyConfig);
        config.addMapConfig(mapConfig);
        return config;
    }
}
