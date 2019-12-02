/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.internal.management.dto;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.ConfigCompatibilityChecker;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapConfigDTOTest {

    @Parameterized.Parameter()
    public MapConfig mapConfig;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {defaultMapConfig()},
                {fullMapConfig()},
        });
    }

    private static final String MAP_NAME = "map-name";
    private static final ConfigCompatibilityChecker.MapConfigChecker MAP_CONFIG_CHECKER
            = new ConfigCompatibilityChecker.MapConfigChecker();

    private static MapConfig defaultMapConfig() {
        return new MapConfig(MAP_NAME);
    }

    private static MapConfig fullMapConfig() {
        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.getEvictionConfig().setEvictionPolicy(EvictionPolicy.RANDOM);
        mapConfig.setBackupCount(2)
                .setAsyncBackupCount(3)
                .setCacheDeserializedValues(CacheDeserializedValues.ALWAYS)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setSplitBrainProtectionName("triple");

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setLocalUpdatePolicy(NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE);
        nearCacheConfig.getPreloaderConfig().setEnabled(false).setStoreIntervalSeconds(11)
                .setStoreInitialDelaySeconds(33).setDirectory("storeHere");
        mapConfig.setNearCacheConfig(nearCacheConfig);

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapStoreConfig.setClassName("map-store-impl-class-name");
        mapConfig.setMapStoreConfig(mapStoreConfig);

        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig();
        mergePolicyConfig.setBatchSize(111);
        mergePolicyConfig.setPolicy(PassThroughMergePolicy.class.getName());

        return mapConfig;
    }

    @Test
    public void cloned_config_with_json_serialization_equals_given_config() {
        MapConfig clonedConfig = cloneWithJsonSerialization(mapConfig);
        assertTrue("Expected: " + mapConfig + ", got:" + clonedConfig,
                MAP_CONFIG_CHECKER.check(mapConfig, clonedConfig));
    }

    private static MapConfig cloneWithJsonSerialization(MapConfig expected) {
        MapConfigDTO dto = new MapConfigDTO(expected);

        JsonObject json = dto.toJson();
        MapConfigDTO deserialized = new MapConfigDTO(null);
        deserialized.fromJson(json);

        return deserialized.getConfig();
    }

    @Test
    public void cloned_config_with_data_serialization_equals_given_config() {
        MapConfig clonedConfig = cloneWithDataSerialization(mapConfig);
        assertTrue("Expected: " + mapConfig + ", got:" + clonedConfig,
                MAP_CONFIG_CHECKER.check(mapConfig, clonedConfig));
    }

    private static MapConfig cloneWithDataSerialization(MapConfig givenConfig) {
        DefaultSerializationServiceBuilder defaultSerializationServiceBuilder
                = new DefaultSerializationServiceBuilder();
        SerializationService ss = defaultSerializationServiceBuilder
                .setVersion(InternalSerializationService.VERSION_1).build();

        MapConfigDTO givenDTO = new MapConfigDTO(givenConfig);
        Data serializedDTO = ss.toData(givenDTO);
        MapConfigDTO deserializedDTO = ss.toObject(serializedDTO);

        return deserializedDTO.getConfig();
    }
}
