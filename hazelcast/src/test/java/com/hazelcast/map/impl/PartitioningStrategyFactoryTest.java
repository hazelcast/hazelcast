/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.FunctionArgument;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.RootCauseMatcher;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
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
        PartitioningStrategy partitioningStrategy = partitioningStrategyFactory.getPartitioningStrategy(mapName, null, null);
        assertNull(partitioningStrategy);
    }

    @Test
    public void whenPartitioningStrategyDefined_getPartitioningStrategy_returnsSameInstance() {
        PartitioningStrategy configuredPartitioningStrategy = new StringPartitioningStrategy();
        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig(configuredPartitioningStrategy);
        PartitioningStrategy partitioningStrategy = partitioningStrategyFactory.getPartitioningStrategy(mapName, cfg, null);
        assertSame(configuredPartitioningStrategy, partitioningStrategy);
    }

    @Test
    public void whenPartitioningStrategyClassDefined_getPartitioningStrategy_returnsNewInstance() {
        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig();
        cfg.setPartitioningStrategyClass("com.hazelcast.partition.strategy.StringPartitioningStrategy");
        PartitioningStrategy partitioningStrategy = partitioningStrategyFactory.getPartitioningStrategy(mapName, cfg, null);
        assertEquals(StringPartitioningStrategy.class, partitioningStrategy.getClass());
    }

    // when a partitioning strategy has already been cached, then another invocation to obtain the partitioning
    // strategy for the same map name retrieves the same instance
    @Test
    public void whenStrategyForMapAlreadyDefined_getPartitioningStrategy_returnsSameInstance() {
        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig();
        cfg.setPartitioningStrategyClass("com.hazelcast.partition.strategy.StringPartitioningStrategy");
        // when we have already obtained the partitioning strategy for a given map name
        PartitioningStrategy partitioningStrategy = partitioningStrategyFactory.getPartitioningStrategy(mapName, cfg, null);
        // then once we get it again with the same arguments, we retrieve the same instance
        PartitioningStrategy cachedPartitioningStrategy = partitioningStrategyFactory.getPartitioningStrategy(mapName, cfg, null);
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
        partitioningStrategyFactory.getPartitioningStrategy(UUID.randomUUID().toString(), cfg, null);
    }

    @Test
    public void whenRemoveInvoked_entryIsRemovedFromCache() {
        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig();
        cfg.setPartitioningStrategyClass("com.hazelcast.partition.strategy.StringPartitioningStrategy");
        PartitioningStrategy partitioningStrategy = partitioningStrategyFactory.getPartitioningStrategy(mapName, cfg, null);
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

    @Test
    public void test_partitioningStrategyWithArgs() {
        partitioningStrategyFactory.cache.clear();
        PartitioningStrategyConfig cfg = new PartitioningStrategyConfig();
        cfg.setPartitioningStrategyClass(SampleStrategyWithArgs.class.getName());
        final List<FunctionArgument> arguments = Arrays.asList(
                new FunctionArgument("valueByte", "Byte", false, new String[]{"1"}),
                new FunctionArgument("arrayByte", "Byte", true, new String[]{"1"}),
                new FunctionArgument("valueShort", "Short", false, new String[]{"2"}),
                new FunctionArgument("arrayShort", "Short", true, new String[]{"2"}),
                new FunctionArgument("valueInteger", "Integer", false, new String[]{"3"}),
                new FunctionArgument("arrayInteger", "Integer", true, new String[]{"3"}),
                new FunctionArgument("valueLong", "Long", false, new String[]{"4"}),
                new FunctionArgument("arrayLong", "Long", true, new String[]{"4"}),
                new FunctionArgument("valueFloat", "Float", false, new String[]{"5.0"}),
                new FunctionArgument("arrayFloat", "Float", true, new String[]{"5.0"}),
                new FunctionArgument("valueDouble", "Double", false, new String[]{"6.0"}),
                new FunctionArgument("arrayDouble", "Double", true, new String[]{"6.0"}),
                new FunctionArgument("valueString", "String", false, new String[]{"seven"}),
                new FunctionArgument("arrayString", "String", true, new String[]{"seven"}),
                new FunctionArgument("valueCharacter", "Character", false, new String[]{"e"}),
                new FunctionArgument("arrayCharacter", "Character", true, new String[]{"e"}),
                new FunctionArgument("valueBoolean", "Boolean", false, new String[]{"true"}),
                new FunctionArgument("arrayBoolean", "Boolean", true, new String[]{"true"})
        );

        PartitioningStrategy partitioningStrategy = partitioningStrategyFactory.getPartitioningStrategy(mapName, cfg, arguments);
        assertEquals(SampleStrategyWithArgs.class, partitioningStrategy.getClass());
        SampleStrategyWithArgs ps = (SampleStrategyWithArgs) partitioningStrategy;
        assertEquals(1L, (long) ps.valueByte);
        assertEquals(2L, (long) ps.valueShort);
        assertEquals(3L, (long) ps.valueInteger);
        assertEquals(4L, (long) ps.valueLong);
        assertEquals(5.0f, ps.valueFloat, 0.01);
        assertEquals(6.0, ps.valueDouble, 0.01);
        assertEquals("seven", ps.valueString);
        assertEquals((Character) 'e', ps.valueCharacter);
        assertTrue(ps.valueBoolean);

        assertArrayEquals(new Byte[]{1}, ps.arrayByte);
        assertArrayEquals(new Short[]{2}, ps.arrayShort);
        assertArrayEquals(new Integer[]{3}, ps.arrayInteger);
        assertArrayEquals(new Long[]{4L}, ps.arrayLong);
        assertArrayEquals(new Float[]{5.0f}, ps.arrayFloat);
        assertArrayEquals(new Double[]{6.0}, ps.arrayDouble);
        assertArrayEquals(new String[]{"seven"}, ps.arrayString);
        assertArrayEquals(new Character[]{'e'}, ps.arrayCharacter);
        assertArrayEquals(new Boolean[]{true}, ps.arrayBoolean);
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

    private static final class SampleStrategyWithArgs implements PartitioningStrategy {
        private Byte valueByte;
        private Byte[] arrayByte;
        private Short valueShort;
        private Short[] arrayShort;
        private Integer valueInteger;
        private Integer[] arrayInteger;
        private Long valueLong;
        private Long[] arrayLong;
        private Float valueFloat;
        private Float[] arrayFloat;
        private Double valueDouble;
        private Double[] arrayDouble;
        private String valueString;
        private String[] arrayString;
        private Character valueCharacter;
        private Character[] arrayCharacter;
        private Boolean valueBoolean;
        private Boolean[] arrayBoolean;

        @SuppressWarnings({"checkstyle:parameterNumber", "checkstyle:RedundantModifier"})
        public SampleStrategyWithArgs(
                final Byte valueByte,
                final Byte[] arrayByte,
                final Short valueShort,
                final Short[] arrayShort,
                final Integer valueInteger,
                final Integer[] arrayInteger,
                final Long valueLong,
                final Long[] arrayLong,
                final Float valueFloat,
                final Float[] arrayFloat,
                final Double valueDouble,
                final Double[] arrayDouble,
                final String valueString,
                final String arrayString,
                final Character valueCharacter,
                final Character[] arrayCharacter,
                final Boolean valueBoolean,
                final Boolean[] arrayBoolean
        ) {
            throw new HazelcastException("Wrong constructor!");
        }

        @SuppressWarnings({"checkstyle:parameterNumber", "checkstyle:RedundantModifier"})
        public SampleStrategyWithArgs(
                final Byte valueByte,
                final Byte[] arrayByte,
                final Short valueShort,
                final Short[] arrayShort,
                final Integer valueInteger,
                final Integer[] arrayInteger,
                final Long valueLong,
                final Long[] arrayLong,
                final Float valueFloat,
                final Float[] arrayFloat,
                final Double valueDouble,
                final Double[] arrayDouble,
                final String valueString,
                final String[] arrayString,
                final Character valueCharacter,
                final Character[] arrayCharacter,
                final Boolean valueBoolean,
                final Boolean[] arrayBoolean
        ) {
            this.valueByte = valueByte;
            this.arrayByte = arrayByte;
            this.valueShort = valueShort;
            this.arrayShort = arrayShort;
            this.valueInteger = valueInteger;
            this.arrayInteger = arrayInteger;
            this.valueLong = valueLong;
            this.arrayLong = arrayLong;
            this.valueFloat = valueFloat;
            this.arrayFloat = arrayFloat;
            this.valueDouble = valueDouble;
            this.arrayDouble = arrayDouble;
            this.valueString = valueString;
            this.arrayString = arrayString;
            this.valueCharacter = valueCharacter;
            this.arrayCharacter = arrayCharacter;
            this.valueBoolean = valueBoolean;
            this.arrayBoolean = arrayBoolean;
        }

        @Override
        public Object getPartitionKey(final Object key) {
            return null;
        }
    }
}
