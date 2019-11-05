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
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.MetadataPolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.json.JsonSerializable;
import com.hazelcast.internal.management.ManagementDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.config.MapConfig.DEFAULT_CACHED_DESERIALIZED_VALUES;
import static com.hazelcast.config.MapConfig.DEFAULT_METADATA_POLICY;
import static com.hazelcast.config.MapConfig.DEFAULT_STATISTICS_ENABLED;
import static com.hazelcast.internal.util.JsonUtil.getBoolean;
import static com.hazelcast.internal.util.JsonUtil.getInt;
import static com.hazelcast.internal.util.JsonUtil.getString;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

/**
 * Serializable adapter for {@link com.hazelcast.config.MapConfig}
 */
public class MapConfigDTO implements JsonSerializable, IdentifiedDataSerializable {

    private MapConfig mapConfig;

    public MapConfigDTO() {
    }

    public MapConfigDTO(MapConfig mapConfig) {
        this.mapConfig = mapConfig;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();

        String name = mapConfig.getName();
        if (!isNullOrEmpty(name)) {
            root.add("name", name);
        }

        String splitBrainProtectionName = mapConfig.getSplitBrainProtectionName();
        if (!isNullOrEmpty(splitBrainProtectionName)) {
            root.add("splitBrainProtectionName", splitBrainProtectionName);
        }

        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        root.add("maxSize", evictionConfig.getSize());
        root.add("maxSizePolicy", evictionConfig.getMaxSizePolicy().toString());
        root.add("evictionPolicy", evictionConfig.getEvictionPolicy().toString());
        root.add("memoryFormat", mapConfig.getInMemoryFormat().toString());
        root.add("cacheDeserializedValues", mapConfig.getCacheDeserializedValues().toString());
        root.add("metadataPolicy", mapConfig.getMetadataPolicy().toString());
        root.add("backupCount", mapConfig.getBackupCount());
        root.add("asyncBackupCount", mapConfig.getAsyncBackupCount());
        root.add("ttl", mapConfig.getTimeToLiveSeconds());
        root.add("maxIdle", mapConfig.getMaxIdleSeconds());
        root.add("readBackupData", mapConfig.isReadBackupData());
        root.add("statisticsEnabled", mapConfig.isStatisticsEnabled());
        root.add("mergePolicy", mapConfig.getMergePolicyConfig().getPolicy());
        root.add("mapStoreConfig", new MapStoreConfigDTO(mapConfig.getMapStoreConfig()).toJson());

        NearCacheConfig nearCacheConfig = mapConfig.getNearCacheConfig();
        if (nearCacheConfig != null) {
            root.add("nearCacheConfig", new NearCacheConfigDTO(nearCacheConfig).toJson());
        }

        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        mapConfig = new MapConfig();

        JsonValue name = json.get("name");
        if (name != null && !name.isNull()) {
            mapConfig.setName(getString(json, "name"));
        }

        JsonValue splitBrainProtectionName = json.get("splitBrainProtectionName");
        if (splitBrainProtectionName != null && !splitBrainProtectionName.isNull()) {
            mapConfig.setSplitBrainProtectionName(getString(json, "splitBrainProtectionName"));
        }

        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig.setSize(getInt(json, "maxSize"));
        evictionConfig.setMaxSizePolicy(MaxSizePolicy.valueOf(getString(json, "maxSizePolicy")));
        evictionConfig.setEvictionPolicy(EvictionPolicy.valueOf(getString(json, "evictionPolicy")));
        mapConfig.setInMemoryFormat(InMemoryFormat.valueOf(getString(json, "memoryFormat")));
        mapConfig.setCacheDeserializedValues(CacheDeserializedValues.valueOf(
                getString(json, "cacheDeserializedValues", DEFAULT_CACHED_DESERIALIZED_VALUES.name())));
        mapConfig.setMetadataPolicy(
                MetadataPolicy.valueOf(getString(json, "metadataPolicy", DEFAULT_METADATA_POLICY.name())));
        mapConfig.setBackupCount(getInt(json, "backupCount"));
        mapConfig.setAsyncBackupCount(getInt(json, "asyncBackupCount"));
        mapConfig.setTimeToLiveSeconds(getInt(json, "ttl"));
        mapConfig.setMaxIdleSeconds(getInt(json, "maxIdle"));
        mapConfig.setReadBackupData(getBoolean(json, "readBackupData"));
        mapConfig.setStatisticsEnabled(
                getBoolean(json, "statisticsEnabled", DEFAULT_STATISTICS_ENABLED));

        String mergePolicy = getString(json, "mergePolicy", null);
        if (mergePolicy != null) {
            mapConfig.getMergePolicyConfig().setPolicy(mergePolicy);
        }

        JsonValue mapStoreConfig = json.get("mapStoreConfig");
        if (mapStoreConfig != null && !mapStoreConfig.isNull()) {
            MapStoreConfigDTO mapStoreConfigDTO = new MapStoreConfigDTO();
            mapStoreConfigDTO.fromJson(mapStoreConfig.asObject());
            mapConfig.setMapStoreConfig(mapStoreConfigDTO.getConfig());
        }

        JsonValue nearCacheConfig = json.get("nearCacheConfig");
        if (nearCacheConfig != null && !nearCacheConfig.isNull()) {
            NearCacheConfigDTO nearCacheConfigDTO = new NearCacheConfigDTO();
            nearCacheConfigDTO.fromJson(nearCacheConfig.asObject());
            mapConfig.setNearCacheConfig(nearCacheConfigDTO.getConfig());
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapConfig.getName());
        out.writeUTF(mapConfig.getSplitBrainProtectionName());
        out.writeInt(mapConfig.getEvictionConfig().getSize());
        out.writeUTF(mapConfig.getEvictionConfig().getMaxSizePolicy().toString());
        out.writeUTF(mapConfig.getEvictionConfig().getEvictionPolicy().toString());
        out.writeUTF(mapConfig.getInMemoryFormat().toString());
        out.writeUTF(mapConfig.getCacheDeserializedValues().toString());
        out.writeUTF(mapConfig.getMetadataPolicy().toString());
        out.writeInt(mapConfig.getBackupCount());
        out.writeInt(mapConfig.getAsyncBackupCount());
        out.writeInt(mapConfig.getTimeToLiveSeconds());
        out.writeInt(mapConfig.getMaxIdleSeconds());
        out.writeBoolean(mapConfig.isReadBackupData());
        out.writeBoolean(mapConfig.isStatisticsEnabled());
        out.writeObject(mapConfig.getMergePolicyConfig());
        out.writeObject(mapConfig.getMapStoreConfig());
        out.writeObject(mapConfig.getNearCacheConfig());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapConfig = new MapConfig();
        mapConfig.setName(in.readUTF());
        mapConfig.setSplitBrainProtectionName(in.readUTF());
        EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
        evictionConfig.setSize(in.readInt());
        evictionConfig.setMaxSizePolicy(MaxSizePolicy.valueOf(in.readUTF()));
        evictionConfig.setEvictionPolicy(EvictionPolicy.valueOf(in.readUTF()));
        mapConfig.setInMemoryFormat(InMemoryFormat.valueOf(in.readUTF()));
        mapConfig.setCacheDeserializedValues(CacheDeserializedValues.valueOf(in.readUTF()));
        mapConfig.setMetadataPolicy(MetadataPolicy.valueOf(in.readUTF()));
        mapConfig.setBackupCount(in.readInt());
        mapConfig.setAsyncBackupCount(in.readInt());
        mapConfig.setTimeToLiveSeconds(in.readInt());
        mapConfig.setMaxIdleSeconds(in.readInt());
        mapConfig.setReadBackupData(in.readBoolean());
        mapConfig.setStatisticsEnabled(in.readBoolean());
        mapConfig.setMergePolicyConfig(in.readObject());
        mapConfig.setMapStoreConfig(in.readObject());
        mapConfig.setNearCacheConfig(in.readObject());
    }

    public MapConfig getConfig() {
        return mapConfig;
    }

    @Override
    public int getFactoryId() {
        return ManagementDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ManagementDataSerializerHook.MAP_CONFIG_DTO;
    }
}
