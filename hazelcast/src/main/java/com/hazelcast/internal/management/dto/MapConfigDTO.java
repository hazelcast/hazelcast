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

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.JsonSerializable;
import com.hazelcast.internal.management.ManagementDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.util.JsonUtil.getBoolean;
import static com.hazelcast.util.JsonUtil.getInt;
import static com.hazelcast.util.JsonUtil.getString;

/**
 * Serializable adapter for {@link com.hazelcast.config.MapConfig}
 */
public class MapConfigDTO implements JsonSerializable, IdentifiedDataSerializable {

    private MapConfig config;

    public MapConfigDTO() {
    }

    public MapConfigDTO(MapConfig mapConfig) {
        this.config = mapConfig;
    }

    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("name", config.getName());
        root.add("memoryFormat", config.getInMemoryFormat().toString());
        root.add("backupCount", config.getBackupCount());
        root.add("asyncBackupCount", config.getAsyncBackupCount());
        root.add("ttl", config.getTimeToLiveSeconds());
        root.add("maxIdle", config.getMaxIdleSeconds());
        root.add("maxSize", config.getMaxSizeConfig().getSize());
        root.add("maxSizePolicy", config.getMaxSizeConfig().getMaxSizePolicy().toString());
        root.add("readBackupData", config.isReadBackupData());
        root.add("evictionPolicy", config.getEvictionPolicy().name());
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        config = new MapConfig();
        config.setName(getString(json, "name"));
        config.setInMemoryFormat(InMemoryFormat.valueOf(getString(json, "memoryFormat")));
        config.setBackupCount(getInt(json, "backupCount"));
        config.setAsyncBackupCount(getInt(json, "asyncBackupCount"));
        config.setTimeToLiveSeconds(getInt(json, "ttl"));
        config.setMaxIdleSeconds(getInt(json, "maxIdle"));
        config.setMaxSizeConfig(new MaxSizeConfig().setSize(getInt(json, "maxSize"))
                .setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.valueOf(getString(json, "maxSizePolicy"))));
        config.setReadBackupData(getBoolean(json, "readBackupData"));
        config.setEvictionPolicy(EvictionPolicy.valueOf(getString(json, "evictionPolicy")));
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        config = new MapConfig();
        config.setName(in.readUTF());
        config.setInMemoryFormat(InMemoryFormat.valueOf(in.readUTF()));
        config.setBackupCount(in.readInt());
        config.setAsyncBackupCount(in.readInt());
        config.setTimeToLiveSeconds(in.readInt());
        config.setMaxIdleSeconds(in.readInt());
        config.setMaxSizeConfig(
                new MaxSizeConfig()
                        .setSize(in.readInt())
                        .setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.valueOf(in.readUTF())));
        config.setReadBackupData(in.readBoolean());
        config.setEvictionPolicy(EvictionPolicy.valueOf(in.readUTF()));
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(config.getName());
        out.writeUTF(config.getInMemoryFormat().toString());
        out.writeInt(config.getBackupCount());
        out.writeInt(config.getAsyncBackupCount());
        out.writeInt(config.getTimeToLiveSeconds());
        out.writeInt(config.getMaxIdleSeconds());
        out.writeInt(config.getMaxSizeConfig().getSize());
        out.writeUTF(config.getMaxSizeConfig().getMaxSizePolicy().toString());
        out.writeBoolean(config.isReadBackupData());
        out.writeUTF(config.getEvictionPolicy().name());
    }

    public MapConfig getMapConfig() {
        return config;
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
