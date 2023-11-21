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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddMapConfigCodec;
import com.hazelcast.config.DataPersistenceConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningAttributeConfig;
import com.hazelcast.config.TieredStoreConfig;
import com.hazelcast.memory.Capacity;
import com.hazelcast.memory.MemoryUnit;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class AddMapConfigMessageTaskTest extends ConfigMessageTaskTest {
    @Test
    public void doNotThrowException_whenNullValuesProvidedForNullableFields() {
        MapConfig mapConfig = new MapConfig("my-map");
        ClientMessage addMapConfigClientMessage = DynamicConfigAddMapConfigCodec.encodeRequest(
                mapConfig.getName(),
                mapConfig.getBackupCount(),
                mapConfig.getAsyncBackupCount(),
                mapConfig.getTimeToLiveSeconds(),
                mapConfig.getMaxIdleSeconds(),
                null,
                mapConfig.isReadBackupData(),
                mapConfig.getCacheDeserializedValues().name(),
                mapConfig.getMergePolicyConfig().getPolicy(),
                mapConfig.getMergePolicyConfig().getBatchSize(),
                mapConfig.getInMemoryFormat().name(),
                null,
                null,
                mapConfig.isStatisticsEnabled(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                mapConfig.getMetadataPolicy().getId(),
                mapConfig.isPerEntryStatsEnabled(),
                mapConfig.getDataPersistenceConfig(),
                mapConfig.getTieredStoreConfig(),
                null
        );
        AddMapConfigMessageTask addMapConfigMessageTask = new AddMapConfigMessageTask(addMapConfigClientMessage, mockNode, mockConnection);
        addMapConfigMessageTask.run();
        MapConfig transmittedMapConfig = (MapConfig) addMapConfigMessageTask.getConfig();
        assertEquals(mapConfig, transmittedMapConfig);
    }

    @Test
    public void testDataPersistenceAndTieredStoreConfigTransmittedCorrectly() {
        MapConfig mapConfig = new MapConfig("my-map");
        DataPersistenceConfig dataPersistenceConfig = new DataPersistenceConfig();
        dataPersistenceConfig.setEnabled(true);
        dataPersistenceConfig.setFsync(true);
        mapConfig.setDataPersistenceConfig(dataPersistenceConfig);
        TieredStoreConfig tieredStoreConfig = mapConfig.getTieredStoreConfig();
        tieredStoreConfig.setEnabled(true);
        tieredStoreConfig.getMemoryTierConfig().setCapacity(Capacity.of(1L, MemoryUnit.GIGABYTES));
        tieredStoreConfig.getDiskTierConfig().setEnabled(true).setDeviceName("null-device");

        ClientMessage addMapConfigClientMessage = DynamicConfigAddMapConfigCodec.encodeRequest(
                mapConfig.getName(),
                mapConfig.getBackupCount(),
                mapConfig.getAsyncBackupCount(),
                mapConfig.getTimeToLiveSeconds(),
                mapConfig.getMaxIdleSeconds(),
                null,
                mapConfig.isReadBackupData(),
                mapConfig.getCacheDeserializedValues().name(),
                mapConfig.getMergePolicyConfig().getPolicy(),
                mapConfig.getMergePolicyConfig().getBatchSize(),
                mapConfig.getInMemoryFormat().name(),
                null,
                null,
                mapConfig.isStatisticsEnabled(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                mapConfig.getMetadataPolicy().getId(),
                mapConfig.isPerEntryStatsEnabled(),
                mapConfig.getDataPersistenceConfig(),
                mapConfig.getTieredStoreConfig(),
                null
        );
        AddMapConfigMessageTask addMapConfigMessageTask = new AddMapConfigMessageTask(addMapConfigClientMessage, mockNode, mockConnection);
        addMapConfigMessageTask.run();
        MapConfig transmittedMapConfig = (MapConfig) addMapConfigMessageTask.getConfig();
        assertEquals(mapConfig, transmittedMapConfig);
    }

    @Test
    public void testPartitioningAttributeConfigsTransmittedCorrectly() {
        MapConfig mapConfig = new MapConfig("my-map");
        mapConfig.setPartitioningAttributeConfigs(Arrays.asList(
                new PartitioningAttributeConfig("attr1"),
                new PartitioningAttributeConfig("attr2")
        ));

        ClientMessage addMapConfigClientMessage = DynamicConfigAddMapConfigCodec.encodeRequest(
                mapConfig.getName(),
                mapConfig.getBackupCount(),
                mapConfig.getAsyncBackupCount(),
                mapConfig.getTimeToLiveSeconds(),
                mapConfig.getMaxIdleSeconds(),
                null,
                mapConfig.isReadBackupData(),
                mapConfig.getCacheDeserializedValues().name(),
                mapConfig.getMergePolicyConfig().getPolicy(),
                mapConfig.getMergePolicyConfig().getBatchSize(),
                mapConfig.getInMemoryFormat().name(),
                null,
                null,
                mapConfig.isStatisticsEnabled(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                mapConfig.getMetadataPolicy().getId(),
                mapConfig.isPerEntryStatsEnabled(),
                mapConfig.getDataPersistenceConfig(),
                mapConfig.getTieredStoreConfig(),
                mapConfig.getPartitioningAttributeConfigs()
        );
        AddMapConfigMessageTask addMapConfigMessageTask = new AddMapConfigMessageTask(addMapConfigClientMessage, mockNode, mockConnection);
        addMapConfigMessageTask.run();
        MapConfig transmittedMapConfig = (MapConfig) addMapConfigMessageTask.getConfig();
        assertEquals(mapConfig, transmittedMapConfig);
    }
}
