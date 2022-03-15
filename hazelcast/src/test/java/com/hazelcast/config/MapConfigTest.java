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

package com.hazelcast.config;

import com.hazelcast.internal.config.MapConfigReadOnly;
import com.hazelcast.internal.config.MapPartitionLostListenerConfigReadOnly;
import com.hazelcast.internal.config.MapStoreConfigReadOnly;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.EventListener;
import java.util.List;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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
        assertEquals(MapConfig.DEFAULT_MAX_SIZE,
                new MapConfig().getEvictionConfig().getSize());
    }

    @Test
    public void testSetMaxSize() {
        assertEquals(1234, new MapConfig().getEvictionConfig().setSize(1234).getSize());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetMaxSizeCannotBeNegative() {
        new MapConfig().getEvictionConfig().setSize(-1);
    }

    @Test
    public void testGetEvictionPolicy() {
        assertEquals(MapConfig.DEFAULT_EVICTION_POLICY,
                new MapConfig().getEvictionConfig().getEvictionPolicy());
    }

    @Test
    public void testSetEvictionPolicy() {
        assertEquals(EvictionPolicy.LRU, new MapConfig().getEvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LRU)
                .getEvictionPolicy());
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
    public void configSetsForDefaultAlwaysIssue466() {
        Config config = new XmlConfigBuilder().build();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setWriteDelaySeconds(0);
        mapStoreConfig.setClassName("com.hazelcast.examples.DummyStore");
        config.getMapConfig("test").setMapStoreConfig(mapStoreConfig);
        config.getMapConfig("default").setMapStoreConfig(null);
        assertNotNull(config.getMapConfig("test").getMapStoreConfig());
        assertNull(config.getMapConfig("default").getMapStoreConfig());
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
        // max allowed is 6
        config.setAsyncBackupCount(200);
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
        // max allowed is 6
        config.setBackupCount(200);
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
        MapConfig mapConfig = new MapConfig();
        MapPartitionLostListener listener = mock(MapPartitionLostListener.class);
        mapConfig.addMapPartitionLostListenerConfig(new MapPartitionLostListenerConfig(listener));
        MapPartitionLostListenerConfig listenerConfig = new MapPartitionLostListenerConfig();
        listenerConfig.setImplementation(listener);
        mapConfig.addMapPartitionLostListenerConfig(listenerConfig);

        List<MapPartitionLostListenerConfig> listenerConfigs = mapConfig.getPartitionLostListenerConfigs();
        assertEquals(2, listenerConfigs.size());
        assertEquals(listener, listenerConfigs.get(0).getImplementation());
        assertEquals(listener, listenerConfigs.get(1).getImplementation());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMapPartitionLostListenerReadOnlyConfig_withClassName() {
        MapPartitionLostListenerConfigReadOnly readOnly
                = new MapPartitionLostListenerConfigReadOnly(new MapPartitionLostListenerConfig());
        readOnly.setClassName("com.hz");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMapPartitionLostListenerReadOnlyConfig_withImplementation() {
        MapPartitionLostListener listener = mock(MapPartitionLostListener.class);
        MapPartitionLostListenerConfig listenerConfig = new MapPartitionLostListenerConfig(listener);
        MapPartitionLostListenerConfigReadOnly readOnly = new MapPartitionLostListenerConfigReadOnly(listenerConfig);
        assertEquals(listener, readOnly.getImplementation());
        readOnly.setImplementation(mock(MapPartitionLostListener.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMapPartitionLostListenerReadOnlyConfig_withEventListenerImplementation() {
        MapPartitionLostListenerConfigReadOnly readOnly
                = new MapPartitionLostListenerConfigReadOnly(new MapPartitionLostListenerConfig());
        readOnly.setImplementation(mock(EventListener.class));
    }

    @Test
    public void testMapPartitionLostListener_equalsWithClassName() {
        MapPartitionLostListenerConfig config1 = new MapPartitionLostListenerConfig();
        config1.setClassName("com.hz");

        MapPartitionLostListenerConfig config2 = new MapPartitionLostListenerConfig();
        config2.setClassName("com.hz");

        MapPartitionLostListenerConfig config3 = new MapPartitionLostListenerConfig();
        config3.setClassName("com.hz2");

        assertEquals(config1, config2);
        assertNotEquals(config1, config3);
        assertNotEquals(config2, config3);
    }

    @Test
    public void testMapPartitionLostListener_equalsWithImplementation() {
        MapPartitionLostListener listener = mock(MapPartitionLostListener.class);

        MapPartitionLostListenerConfig config1 = new MapPartitionLostListenerConfig();
        config1.setImplementation(listener);

        MapPartitionLostListenerConfig config2 = new MapPartitionLostListenerConfig();
        config2.setImplementation(listener);

        MapPartitionLostListenerConfig config3 = new MapPartitionLostListenerConfig();

        assertEquals(config1, config2);
        assertNotEquals(config1, config3);
        assertNotEquals(config2, config3);
    }

    @Test
    @Ignore(value = "this MapStoreConfig does not override equals/hashcode -> this cannot pass right now")
    public void givenSetCacheDeserializedValuesIsINDEX_ONLY_whenComparedWithOtherConfigWhereCacheIsINDEX_ONLY_thenReturnTrue() {
        // given
        MapConfig mapConfig = new MapConfig();
        mapConfig.setCacheDeserializedValues(CacheDeserializedValues.INDEX_ONLY);

        // when
        MapConfig otherMapConfig = new MapConfig();
        otherMapConfig.setCacheDeserializedValues(CacheDeserializedValues.INDEX_ONLY);

        // then
        assertEquals(mapConfig, otherMapConfig);
    }

    @Test
    public void givenDefaultConfig_whenSerializedAndDeserialized_noExceptionIsThrown() {
        MapConfig mapConfig = new MapConfig();
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        Data data = serializationService.toData(mapConfig);
        serializationService.toObject(data);
    }

    @Test
    public void testSetMergePolicyConfig() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(PassThroughMergePolicy.class.getName())
                .setBatchSize(2342);

        MapConfig config = new MapConfig();
        config.setMergePolicyConfig(mergePolicyConfig);

        assertEquals(PassThroughMergePolicy.class.getName(), config.getMergePolicyConfig().getPolicy());
        assertEquals(2342, config.getMergePolicyConfig().getBatchSize());
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(MapConfig.class)
                .suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS)
                .withPrefabValues(EvictionConfig.class,
                        new EvictionConfig().setSize(300).setMaxSizePolicy(MaxSizePolicy.PER_PARTITION),
                        new EvictionConfig().setSize(100).setMaxSizePolicy(MaxSizePolicy.PER_NODE))
                .withPrefabValues(MapStoreConfig.class,
                        new MapStoreConfig().setEnabled(true).setClassName("red"),
                        new MapStoreConfig().setEnabled(true).setClassName("black"))
                .withPrefabValues(NearCacheConfig.class,
                        new NearCacheConfig().setTimeToLiveSeconds(10)
                                .setMaxIdleSeconds(20)
                                .setInvalidateOnChange(false)
                                .setInMemoryFormat(InMemoryFormat.BINARY),
                        new NearCacheConfig().setTimeToLiveSeconds(15)
                                .setMaxIdleSeconds(25)
                                .setInvalidateOnChange(true)
                                .setInMemoryFormat(InMemoryFormat.OBJECT))
                .withPrefabValues(WanReplicationRef.class,
                        new WanReplicationRef().setName("red"),
                        new WanReplicationRef().setName("black"))
                .withPrefabValues(PartitioningStrategyConfig.class,
                        new PartitioningStrategyConfig("red"),
                        new PartitioningStrategyConfig("black"))
                .withPrefabValues(MapConfigReadOnly.class,
                        new MapConfigReadOnly(new MapConfig("red")),
                        new MapConfigReadOnly(new MapConfig("black")))
                .withPrefabValues(MergePolicyConfig.class,
                        new MergePolicyConfig(PutIfAbsentMergePolicy.class.getName(), 100),
                        new MergePolicyConfig(DiscardMergePolicy.class.getName(), 200))
                .verify();
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void testDefaultHashCode() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.hashCode();
    }
}
