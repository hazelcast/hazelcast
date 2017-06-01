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

package com.hazelcast.config;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.InputStream;

import static com.hazelcast.config.EvictionConfig.DEFAULT_MAX_ENTRY_COUNT;
import static com.hazelcast.config.EvictionConfig.DEFAULT_MAX_ENTRY_COUNT_FOR_ON_HEAP_MAP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConfigTest extends HazelcastTestSupport {

    private static final int CUSTOM_EVICTION_SIZE = 23;

    /**
     * Tests that the order of configuration creation matters.
     * <ul>
     * <li>Configurations which are created before the "default" configuration do not inherit from it.</li>
     * <li>Configurations which are created after the "default" configuration do inherit from it.</li>
     * </ul>
     */
    @Test
    public void testInheritanceFromDefaultConfig() {
        assertNotEquals("Expected that the default in-memory format is not OBJECT",
                MapConfig.DEFAULT_IN_MEMORY_FORMAT, InMemoryFormat.OBJECT);

        Config config = new Config();
        config.getMapConfig("myBinaryMap")
                .setBackupCount(3);
        config.getMapConfig("default")
                .setInMemoryFormat(InMemoryFormat.OBJECT);
        config.getMapConfig("myObjectMap")
                .setBackupCount(5);

        HazelcastInstance hz = createHazelcastInstance(config);

        MapConfig binaryMapConfig = hz.getConfig().findMapConfig("myBinaryMap");
        assertEqualsStringFormat("Expected %d sync backups, but found %d", 3, binaryMapConfig.getBackupCount());
        assertEqualsStringFormat("Expected %s in-memory format, but found %s",
                MapConfig.DEFAULT_IN_MEMORY_FORMAT, binaryMapConfig.getInMemoryFormat());

        MapConfig objectMapConfig = hz.getConfig().findMapConfig("myObjectMap");
        assertEqualsStringFormat("Expected %d sync backups, but found %d", 5, objectMapConfig.getBackupCount());
        assertEqualsStringFormat("Expected %s in-memory format, but found %s",
                InMemoryFormat.OBJECT, objectMapConfig.getInMemoryFormat());
    }

    /**
     * Checks the {@link EvictionConfig#getSize()} default values with programmatic configuration.
     * <p>
     * This test uses the out-of-the-box default configuration, to ensure the default values were not modified.
     */
    @Test
    public void testEvictionConfigSize_withOutOfTheBoxDefaultConfigs() {
        Config config = new Config();

        assertNull(config.getMapConfig("default").getNearCacheConfig());
        assertEvictionConfigSize(DEFAULT_MAX_ENTRY_COUNT, config.getCacheConfig("default"));

        assertNull(config.findMapConfig("default").getNearCacheConfig());
        assertEvictionConfigSize(DEFAULT_MAX_ENTRY_COUNT, config.findCacheConfig("default"));
    }

    /**
     * Checks the {@link EvictionConfig#getSize()} default values with programmatic configuration.
     * <p>
     * This test modifies the default configuration, so there is a {@link NearCacheConfig} for maps.
     */
    @Test
    public void testEvictionConfigSize_shouldMatchDataStructureDefaults() {
        EvictionConfig customEvictionConfig = new EvictionConfig()
                .setSize(CUSTOM_EVICTION_SIZE);

        Config config = new Config();

        config.getMapConfig("default")
                .setNearCacheConfig(new NearCacheConfig());

        config.getMapConfig("myMap")
                .setNearCacheConfig(new NearCacheConfig());

        config.getMapConfig("myCustomMap")
                .setNearCacheConfig(new NearCacheConfig()
                        .setEvictionConfig(customEvictionConfig));

        config.getMapConfig("myNativeNearCacheMap")
                .setNearCacheConfig(new NearCacheConfig()
                        .setInMemoryFormat(InMemoryFormat.NATIVE));

        config.getMapConfig("myCustomNativeNearCacheMap")
                .setNearCacheConfig(new NearCacheConfig()
                        .setInMemoryFormat(InMemoryFormat.NATIVE)
                        .setEvictionConfig(customEvictionConfig));

        config.getCacheConfig("default")
                .setEvictionConfig(new EvictionConfig());

        config.getCacheConfig("myCache")
                .setEvictionConfig(new EvictionConfig());

        config.getCacheConfig("myCustomCache")
                .setEvictionConfig(customEvictionConfig);

        assertEvictionConfigSize(config);
    }

    /**
     * Checks the {@link EvictionConfig#getSize()} default values with declarative configuration.
     * <p>
     * This test modifies the default configuration, so there is a {@link NearCacheConfig} for maps.
     */
    @Test
    public void testEvictionConfigSize_shouldMatchDataStructureDefaults_viaXML() {
        InputStream is = getClass().getResourceAsStream("/com/hazelcast/config/hazelcast-eviction-config-size.xml");
        Config config = new XmlConfigBuilder(is).build();

        assertEvictionConfigSize(config);
    }

    /**
     * Since the Near Cache implementations have been unified, we have different default values
     * for the {@link EvictionConfig#getSize()}:
     * <ul>
     * <li>{@link Integer#MAX_VALUE} for on-heap IMaps</li>
     * <li>{@code 10000} for JCache and off-heap IMaps</li>
     * </ul>
     * We use {@link NearCacheConfigAccessor#initDefaultMaxSizeForOnHeapMaps(NearCacheConfig)}
     * to set the correct default value for IMap and JCache, if no custom value was set.
     * <p>
     * This method asserts that {@link Config#getMapConfig(String)} and {@link Config#getCacheConfig(String)}
     * return {@link EvictionConfig#DEFAULT_MAX_ENTRY_COUNT}, if the eviction size was not modified.
     * <p>
     * This method asserts that {@link Config#findMapConfig(String)} and {@link Config#findCacheConfig(String)}
     * return {@link EvictionConfig#DEFAULT_MAX_ENTRY_COUNT_FOR_ON_HEAP_MAP} for on-heap maps and
     * {@link EvictionConfig#DEFAULT_MAX_ENTRY_COUNT} for off-heap maps and caches.
     */
    private static void assertEvictionConfigSize(Config config) {
        // the getFooConfig() methods should not change the default value,
        // since the user is allowed to retrieve and change those configs multiple times
        // and might not have set the final in-memory format for a map
        assertEvictionConfigSize(DEFAULT_MAX_ENTRY_COUNT, config.getMapConfig("default"));
        assertEvictionConfigSize(DEFAULT_MAX_ENTRY_COUNT, config.getMapConfig("myMap"));

        assertEvictionConfigSize(DEFAULT_MAX_ENTRY_COUNT, config.getMapConfig("myNativeNearCacheMap"));
        assertEvictionConfigSize(DEFAULT_MAX_ENTRY_COUNT, config.getCacheConfig("default"));
        assertEvictionConfigSize(DEFAULT_MAX_ENTRY_COUNT, config.getCacheConfig("myCache"));

        assertEvictionConfigSize(CUSTOM_EVICTION_SIZE, config.getMapConfig("myCustomMap"));
        assertEvictionConfigSize(CUSTOM_EVICTION_SIZE, config.getMapConfig("myCustomNativeNearCacheMap"));
        assertEvictionConfigSize(CUSTOM_EVICTION_SIZE, config.getCacheConfig("myCustomCache"));

        // the findFooConfig() methods should have the fixed values set,
        // since they are the read-only settings used by Hazelcast,
        // but the fix should just be applied for on-heap maps, not NATIVE maps or caches
        assertEvictionConfigSize(DEFAULT_MAX_ENTRY_COUNT_FOR_ON_HEAP_MAP, config.findMapConfig("default"));
        assertEvictionConfigSize(DEFAULT_MAX_ENTRY_COUNT_FOR_ON_HEAP_MAP, config.findMapConfig("myMap"));

        assertEvictionConfigSize(DEFAULT_MAX_ENTRY_COUNT, config.findMapConfig("myNativeNearCacheMap"));
        assertEvictionConfigSize(DEFAULT_MAX_ENTRY_COUNT, config.findCacheConfig("default"));
        assertEvictionConfigSize(DEFAULT_MAX_ENTRY_COUNT, config.findCacheConfig("myCache"));

        assertEvictionConfigSize(CUSTOM_EVICTION_SIZE, config.findMapConfig("myCustomMap"));
        assertEvictionConfigSize(CUSTOM_EVICTION_SIZE, config.findMapConfig("myCustomNativeNearCacheMap"));
        assertEvictionConfigSize(CUSTOM_EVICTION_SIZE, config.findCacheConfig("myCustomCache"));
    }

    private static void assertEvictionConfigSize(int expectedSize, MapConfig mapConfig) {
        assertEvictionConfigSize(expectedSize, mapConfig.getNearCacheConfig().getEvictionConfig());
    }

    private static void assertEvictionConfigSize(int expectedSize, CacheSimpleConfig cacheConfig) {
        assertEvictionConfigSize(expectedSize, cacheConfig.getEvictionConfig());
    }

    private static void assertEvictionConfigSize(int expectedSize, EvictionConfig evictionConfig) {
        assertEquals("The EvictionConfig size didn't match the expected value", expectedSize, evictionConfig.getSize());
    }
}
