/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapConfigTest {

    /**
     * Test method for {@link com.hazelcast.config.MapConfig#getName()}.
     */
    @Test
    public void testGetName() {
        assertNull(new MapConfig().getName());
    }

    /**
     * Test method for {@link com.hazelcast.config.MapConfig#setName(java.lang.String)}.
     */
    @Test
    public void testSetName() {
        assertEquals("map-test-name", new MapConfig().setName("map-test-name").getName());
    }

    /**
     * Test method for {@link com.hazelcast.config.MapConfig#getBackupCount()}.
     */
    @Test
    public void testGetBackupCount() {
        assertEquals(MapConfig.DEFAULT_BACKUP_COUNT, new MapConfig().getBackupCount());
    }

    /**
     * Test method for {@link com.hazelcast.config.MapConfig#setBackupCount(int)}.
     */
    @Test
    public void testSetBackupCount() {
        assertEquals(0, new MapConfig().setBackupCount(0).getBackupCount());
        assertEquals(1, new MapConfig().setBackupCount(1).getBackupCount());
        assertEquals(2, new MapConfig().setBackupCount(2).getBackupCount());
        assertEquals(3, new MapConfig().setBackupCount(3).getBackupCount());
    }

    /**
     * Test method for {@link com.hazelcast.config.MapConfig#setBackupCount(int)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetBackupCountLowerLimit() {
        new MapConfig().setBackupCount(MapConfig.MIN_BACKUP_COUNT - 1);
    }

    /**
     * Test method for {@link com.hazelcast.config.MapConfig#getEvictionPercentage()}.
     */
    @Test
    public void testGetEvictionPercentage() {
        assertEquals(MapConfig.DEFAULT_EVICTION_PERCENTAGE, new MapConfig().getEvictionPercentage());
    }

    /**
     * Test method for {@link com.hazelcast.config.MapConfig#setEvictionPercentage(int)}.
     */
    @Test
    public void testSetEvictionPercentage() {
        assertEquals(50, new MapConfig().setEvictionPercentage(50).getEvictionPercentage());
    }

    /**
     * Test method for {@link com.hazelcast.config.MapConfig#setEvictionPercentage(int)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetEvictionPercentageLowerLimit() {
        new MapConfig().setEvictionPercentage(MapConfig.MIN_EVICTION_PERCENTAGE - 1);
    }

    /**
     * Test method for {@link com.hazelcast.config.MapConfig#setEvictionPercentage(int)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSetEvictionPercentageUpperLimit() {
        new MapConfig().setEvictionPercentage(MapConfig.MAX_EVICTION_PERCENTAGE + 1);
    }

    /**
     * Test method for {@link com.hazelcast.config.MapConfig#getTimeToLiveSeconds()}.
     */
    @Test
    public void testGetTimeToLiveSeconds() {
        assertEquals(MapConfig.DEFAULT_TTL_SECONDS, new MapConfig().getTimeToLiveSeconds());
    }

    /**
     * Test method for {@link com.hazelcast.config.MapConfig#setTimeToLiveSeconds(int)}.
     */
    @Test
    public void testSetTimeToLiveSeconds() {
        assertEquals(1234, new MapConfig().setTimeToLiveSeconds(1234).getTimeToLiveSeconds());
    }

    /**
     * Test method for {@link com.hazelcast.config.MapConfig#getMaxIdleSeconds()}.
     */
    @Test
    public void testGetMaxIdleSeconds() {
        assertEquals(MapConfig.DEFAULT_MAX_IDLE_SECONDS, new MapConfig().getMaxIdleSeconds());
    }

    /**
     * Test method for {@link com.hazelcast.config.MapConfig#setMaxIdleSeconds(int)}.
     */
    @Test
    public void testSetMaxIdleSeconds() {
        assertEquals(1234, new MapConfig().setMaxIdleSeconds(1234).getMaxIdleSeconds());
    }

    @Test
    public void testGetMaxSize() {
        assertEquals(MapConfig.DEFAULT_MAX_SIZE, new MapConfig().getMaxSizeConfig().getSize());
    }

    @Test
    public void testSetMaxSize() {
        assertEquals(1234, new MapConfig().getMaxSizeConfig().setSize(1234).getSize());
    }

    @Test
    public void testSetMaxSizeMustBePositive() {
        assertTrue(new MapConfig().getMaxSizeConfig().setSize(-1).getSize() > 0);
    }

    /**
     * Test method for {@link com.hazelcast.config.MapConfig#getEvictionPolicy()}.
     */
    @Test
    public void testGetEvictionPolicy() {
        assertEquals(MapConfig.DEFAULT_EVICTION_POLICY, new MapConfig().getEvictionPolicy());
    }

    @Test
    public void testSetEvictionPolicy() {
        assertEquals(MapConfig.EvictionPolicy.LRU, new MapConfig().setEvictionPolicy(MapConfig.EvictionPolicy.LRU).getEvictionPolicy());
    }

    /**
     * Test method for {@link com.hazelcast.config.MapConfig#getMapStoreConfig()}.
     */
    @Test
    public void testGetMapStoreConfig() {
        assertNull(new MapConfig().getMapStoreConfig());
    }

    /**
     * Test method for {@link com.hazelcast.config.MapConfig#setMapStoreConfig(com.hazelcast.config.MapStoreConfig)}.
     */
    @Test
    public void testSetMapStoreConfig() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        assertEquals(mapStoreConfig, new MapConfig().setMapStoreConfig(mapStoreConfig).getMapStoreConfig());
    }

    /**
     * Test method for {@link com.hazelcast.config.MapConfig#getNearCacheConfig()}.
     */
    @Test
    public void testGetNearCacheConfig() {
        assertNull(new MapConfig().getNearCacheConfig());
    }

    /**
     * Test method for {@link com.hazelcast.config.MapConfig#setNearCacheConfig(com.hazelcast.config.NearCacheConfig)}.
     */
    @Test
    public void testSetNearCacheConfig() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        assertEquals(nearCacheConfig, new MapConfig().setNearCacheConfig(nearCacheConfig).getNearCacheConfig());
    }

    @Test
    public void configSetsForDefaultAllwaysIssue466() {
        Config config = new XmlConfigBuilder().build();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setWriteDelaySeconds(0);
        mapStoreConfig.setClassName("com.hazelcast.examples.DummyStore");
        config.getMapConfig("test").setMapStoreConfig(mapStoreConfig);
        config.getMapConfig("default").setMapStoreConfig(null);
        assertNotNull(config.getMapConfig("test").getMapStoreConfig());
    }

    /**
     * Test method for {@link com.hazelcast.config.MapStoreConfig#setWriteBatchSize(int)}.
     */
    @Test(expected = java.lang.UnsupportedOperationException.class)
    public void testReadOnlyMapStoreConfigSetWriteBatchSize() {
        new MapStoreConfigReadOnly(new MapStoreConfig()).setWriteBatchSize(1);
    }

    /**
     * Test method for {@link com.hazelcast.config.MapStoreConfig#setInitialLoadMode(com.hazelcast.config.MapStoreConfig.InitialLoadMode)}
     */
    @Test(expected = java.lang.UnsupportedOperationException.class)
    public void testReadOnlyMapStoreConfigSetInitialLoadMode() {
        new MapStoreConfigReadOnly(new MapStoreConfig()).setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
    }
}
