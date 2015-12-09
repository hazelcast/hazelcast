/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.EventListener;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapConfigTest {

    @Test
    public void testGetName() {
        assertNull(new MapConfig().getName());
    }

    @Test
    public void testSetName() {
        assertEquals("map-test-name", new MapConfig().setName("map-test-name").getName());
    }

    @Test
    public void testGetBackupCount() {
        assertEquals(MapConfig.DEFAULT_BACKUP_COUNT, new MapConfig().getBackupCount());
    }

    @Test
    public void testSetBackupCount() {
        assertEquals(0, new MapConfig().setBackupCount(0).getBackupCount());
        assertEquals(1, new MapConfig().setBackupCount(1).getBackupCount());
        assertEquals(2, new MapConfig().setBackupCount(2).getBackupCount());
        assertEquals(3, new MapConfig().setBackupCount(3).getBackupCount());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetBackupCountLowerLimit() {
        new MapConfig().setBackupCount(MapConfig.MIN_BACKUP_COUNT - 1);
    }

    @Test
    public void testGetEvictionPercentage() {
        assertEquals(MapConfig.DEFAULT_EVICTION_PERCENTAGE, new MapConfig().getEvictionPercentage());
    }

    @Test
    public void testMinEvictionCheckMillis() throws Exception {
        assertEquals(MapConfig.DEFAULT_MIN_EVICTION_CHECK_MILLIS, new MapConfig().getMinEvictionCheckMillis());
    }

    @Test
    public void testSetEvictionPercentage() {
        assertEquals(50, new MapConfig().setEvictionPercentage(50).getEvictionPercentage());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetEvictionPercentageLowerLimit() {
        new MapConfig().setEvictionPercentage(MapConfig.MIN_EVICTION_PERCENTAGE - 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetEvictionPercentageUpperLimit() {
        new MapConfig().setEvictionPercentage(MapConfig.MAX_EVICTION_PERCENTAGE + 1);
    }

    @Test
    public void testGetTimeToLiveSeconds() {
        assertEquals(MapConfig.DEFAULT_TTL_SECONDS, new MapConfig().getTimeToLiveSeconds());
    }

    @Test
    public void testSetTimeToLiveSeconds() {
        assertEquals(1234, new MapConfig().setTimeToLiveSeconds(1234).getTimeToLiveSeconds());
    }

    @Test
    public void testGetMaxIdleSeconds() {
        assertEquals(MapConfig.DEFAULT_MAX_IDLE_SECONDS, new MapConfig().getMaxIdleSeconds());
    }

    @Test
    public void testSetMaxIdleSeconds() {
        assertEquals(1234, new MapConfig().setMaxIdleSeconds(1234).getMaxIdleSeconds());
    }

    @Test
    public void testGetMaxSize() {
        assertEquals(MaxSizeConfig.DEFAULT_MAX_SIZE, new MapConfig().getMaxSizeConfig().getSize());
    }

    @Test
    public void testSetMaxSize() {
        assertEquals(1234, new MapConfig().getMaxSizeConfig().setSize(1234).getSize());
    }

    @Test
    public void testSetMaxSizeMustBePositive() {
        assertTrue(new MapConfig().getMaxSizeConfig().setSize(-1).getSize() > 0);
    }

    @Test
    public void testGetEvictionPolicy() {
        assertEquals(MapConfig.DEFAULT_EVICTION_POLICY, new MapConfig().getEvictionPolicy());
    }

    @Test
    public void testSetEvictionPolicy() {
        assertEquals(EvictionPolicy.LRU, new MapConfig().setEvictionPolicy(EvictionPolicy.LRU).getEvictionPolicy());
    }

    @Test
    public void testGetMapStoreConfig() {
        MapStoreConfig mapStoreConfig = new MapConfig().getMapStoreConfig();

        assertNotNull(mapStoreConfig);
        assertFalse(mapStoreConfig.isEnabled());
    }

    @Test
    public void testSetMapStoreConfig() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        assertEquals(mapStoreConfig, new MapConfig().setMapStoreConfig(mapStoreConfig).getMapStoreConfig());
    }

    @Test
    public void testGetNearCacheConfig() {
        assertNull(new MapConfig().getNearCacheConfig());
    }

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

    @Test(expected = IllegalArgumentException.class)
    public void setAsyncBackupCount_whenItsNegative() {
        MapConfig config = new MapConfig();
        config.setAsyncBackupCount(-1);
    }

    @Test
    public void setAsyncBackupCount_whenItsZero() {
        MapConfig config = new MapConfig();
        config.setAsyncBackupCount(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setAsyncBackupCount_whenTooLarge() {
        MapConfig config = new MapConfig();
        config.setAsyncBackupCount(200); //max allowed is 6..
    }

    @Test(expected = IllegalArgumentException.class)
    public void setBackupCount_whenItsNegative() {
        MapConfig config = new MapConfig();
        config.setBackupCount(-1);
    }

    @Test
    public void setBackupCount_whenItsZero() {
        MapConfig config = new MapConfig();
        config.setBackupCount(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setBackupCount_tooLarge() {
        MapConfig config = new MapConfig();
        config.setBackupCount(200); //max allowed is 6..
    }

    @Test(expected = java.lang.UnsupportedOperationException.class)
    public void testReadOnlyMapStoreConfigSetWriteBatchSize() {
        new MapStoreConfigReadOnly(new MapStoreConfig()).setWriteBatchSize(1);
    }

    @Test(expected = java.lang.UnsupportedOperationException.class)
    public void testReadOnlyMapStoreConfigSetInitialLoadMode() {
        new MapStoreConfigReadOnly(new MapStoreConfig()).setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
    }

    @Test
    public void testMapPartitionLostListenerConfig() {
        final MapConfig mapConfig = new MapConfig();
        final MapPartitionLostListener listener = mock(MapPartitionLostListener.class);
        mapConfig.addMapPartitionLostListenerConfig(new MapPartitionLostListenerConfig(listener));
        final MapPartitionLostListenerConfig listenerConfig = new MapPartitionLostListenerConfig();
        listenerConfig.setImplementation(listener);
        mapConfig.addMapPartitionLostListenerConfig(listenerConfig);

        List<MapPartitionLostListenerConfig> listenerConfigs = mapConfig.getPartitionLostListenerConfigs();
        assertEquals(2, listenerConfigs.size());
        assertEquals(listener, listenerConfigs.get(0).getImplementation());
        assertEquals(listener, listenerConfigs.get(1).getImplementation());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMapPartitionLostListenerReadOnlyConfig_withClassName() {
        final MapPartitionLostListenerConfigReadOnly readOnly = new MapPartitionLostListenerConfig().getAsReadOnly();
        readOnly.setClassName("com.hz");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMapPartitionLostListenerReadOnlyConfig_withImplementation() {
        final MapPartitionLostListener listener = mock(MapPartitionLostListener.class);
        final MapPartitionLostListenerConfig listenerConfig = new MapPartitionLostListenerConfig(listener);
        final MapPartitionLostListenerConfigReadOnly readOnly = listenerConfig.getAsReadOnly();
        assertEquals(listener, readOnly.getImplementation());
        readOnly.setImplementation(mock(MapPartitionLostListener.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMapPartitionLostListenerReadOnlyConfig_withEventListenerImplementation() {
        final MapPartitionLostListenerConfigReadOnly readOnly = new MapPartitionLostListenerConfig().getAsReadOnly();
        readOnly.setImplementation(mock(EventListener.class));
    }

    @Test
    public void testMapPartitionLostListener_equalsWithClassName() {
        final MapPartitionLostListenerConfig config1 = new MapPartitionLostListenerConfig();
        config1.setClassName("com.hz");

        final MapPartitionLostListenerConfig config2 = new MapPartitionLostListenerConfig();
        config2.setClassName("com.hz");

        final MapPartitionLostListenerConfig config3 = new MapPartitionLostListenerConfig();
        config3.setClassName("com.hz2");

        assertEquals(config1, config2);
        assertNotEquals(config1, config3);
        assertNotEquals(config2, config3);
    }

    @Test
    public void testMapPartitionLostListener_equalsWithImplementation() {
        final MapPartitionLostListener listener = mock(MapPartitionLostListener.class);

        final MapPartitionLostListenerConfig config1 = new MapPartitionLostListenerConfig();
        config1.setImplementation(listener);

        final MapPartitionLostListenerConfig config2 = new MapPartitionLostListenerConfig();
        config2.setImplementation(listener);

        final MapPartitionLostListenerConfig config3 = new MapPartitionLostListenerConfig();

        assertEquals(config1, config2);
        assertNotEquals(config1, config3);
        assertNotEquals(config2, config3);
    }

    @Test(expected = ConfigurationException.class)
    public void givenCacheDeserializedValuesSetToALWAYS_whenSetOptimizeQueriesToFalse_thenThrowConfigurationException() {
        //given
        MapConfig mapConfig = new MapConfig();
        mapConfig.setCacheDeserializedValues(CacheDeserializedValues.ALWAYS);

        //when
        mapConfig.setOptimizeQueries(false);
    }

    @Test(expected = ConfigurationException.class)
    public void givenCacheDeserializedValuesSetToNEVER_whenSetOptimizeQueriesToTrue_thenThrowConfigurationException() {
        //given
        MapConfig mapConfig = new MapConfig();
        mapConfig.setCacheDeserializedValues(CacheDeserializedValues.NEVER);

        //when
        mapConfig.setOptimizeQueries(true);
    }

    @Test
    public void givenCacheDeserializedValuesIsDefault_whenSetOptimizeQueriesToTrue_thenSetCacheDeserializedValuesToALWAYS() {
        //given
        MapConfig mapConfig = new MapConfig();

        //when
        mapConfig.setOptimizeQueries(true);

        //then
        CacheDeserializedValues cacheDeserializedValues = mapConfig.getCacheDeserializedValues();
        assertEquals(CacheDeserializedValues.ALWAYS, cacheDeserializedValues);
    }

    @Test
    public void givenCacheDeserializedValuesIsDefault_thenIsOptimizeQueriesReturnFalse() {
        //given
        MapConfig mapConfig = new MapConfig();

        //then
        boolean optimizeQueries = mapConfig.isOptimizeQueries();
        assertFalse(optimizeQueries);
    }

    @Test
    public void givenCacheDeserializedValuesIsDefault_whenSetCacheDeserializedValuesToALWAYS_thenIsOptimizeQueriesReturnTrue() {
        //given
        MapConfig mapConfig = new MapConfig();

        //when
        mapConfig.setCacheDeserializedValues(CacheDeserializedValues.ALWAYS);

        //then
        boolean optimizeQueries = mapConfig.isOptimizeQueries();
        assertTrue(optimizeQueries);
    }

    @Test
    public void givenCacheDeserializedValuesIsDefault_whenSetCacheDeserializedValuesToNEVER_thenIsOptimizeQueriesReturnFalse() {
        //given
        MapConfig mapConfig = new MapConfig();

        //when
        mapConfig.setCacheDeserializedValues(CacheDeserializedValues.NEVER);

        //then
        boolean optimizeQueries = mapConfig.isOptimizeQueries();
        assertFalse(optimizeQueries);
    }

    @Test(expected = ConfigurationException.class)
    public void givenSetOptimizeQueryIsTrue_whenSetCacheDeserializedValuesToNEVER_thenThrowConfigurationException() {
        //given
        MapConfig mapConfig = new MapConfig();
        mapConfig.setOptimizeQueries(true);

        //when
        mapConfig.setCacheDeserializedValues(CacheDeserializedValues.NEVER);
    }

    @Test(expected = ConfigurationException.class)
    public void givenSetOptimizeQueryIsFalse_whenSetCacheDeserializedValuesToALWAYS_thenThrowConfigurationException() {
        //given
        MapConfig mapConfig = new MapConfig();
        mapConfig.setOptimizeQueries(false);

        //when
        mapConfig.setCacheDeserializedValues(CacheDeserializedValues.ALWAYS);
    }

    @Test(expected = ConfigurationException.class)
    public void givenSetOptimizeQueryIsTrue_whenSetCacheDeserializedValuesToINDEX_ONLY_thenThrowConfigurationException() {
        //given
        MapConfig mapConfig = new MapConfig();
        mapConfig.setOptimizeQueries(true);

        //when
        mapConfig.setCacheDeserializedValues(CacheDeserializedValues.INDEX_ONLY);
    }

    @Test
    @Ignore //this MapStoreConfig does not override equals/hashcode -> this cannot pass right now
    public void givenSetCacheDeserializedValuesIsINDEX_ONLY_whenComparedWithOtherConfigWhereCacheIsINDEX_ONLY_thenReturnTrue() {
        //given
        MapConfig mapConfig = new MapConfig();
        mapConfig.setCacheDeserializedValues(CacheDeserializedValues.INDEX_ONLY);

        //when
        MapConfig otherMapConfig = new MapConfig();
        otherMapConfig.setCacheDeserializedValues(CacheDeserializedValues.INDEX_ONLY);

        //then
        assertEquals(mapConfig, otherMapConfig);
    }



}
