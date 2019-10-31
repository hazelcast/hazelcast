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

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.json.JsonSerializable;
import com.hazelcast.internal.util.StringUtil;

import java.util.Map;
import java.util.Properties;

import static com.hazelcast.internal.util.JsonUtil.getBoolean;
import static com.hazelcast.internal.util.JsonUtil.getInt;
import static com.hazelcast.internal.util.JsonUtil.getObject;
import static com.hazelcast.internal.util.JsonUtil.getString;

class MapStoreConfigDTO implements JsonSerializable {

    private MapStoreConfig mapStoreConfig;

    MapStoreConfigDTO() {
    }

    MapStoreConfigDTO(MapStoreConfig mapStoreConfig) {
        this.mapStoreConfig = mapStoreConfig;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject()
                .add("enabled", mapStoreConfig.isEnabled())
                .add("writeBatchSize", mapStoreConfig.getWriteBatchSize())
                .add("writeDelaySeconds", mapStoreConfig.getWriteDelaySeconds())
                .add("writeCoalescing", mapStoreConfig.isWriteCoalescing())
                .add("initialLoadMode", mapStoreConfig.getInitialLoadMode().toString())
                .add("properties", toJsonObject(mapStoreConfig.getProperties()));

        String className = mapStoreConfig.getClassName();
        if (!StringUtil.isNullOrEmpty(className)) {
            root.add("className", className);
        }

        String factoryClassName = mapStoreConfig.getFactoryClassName();
        if (!StringUtil.isNullOrEmpty(factoryClassName)) {
            root.add("factoryClassName", factoryClassName);
        }

        return root;
    }

    private static JsonObject toJsonObject(Properties properties) {
        JsonObject object = new JsonObject();
        for (Map.Entry<Object, Object> property : properties.entrySet()) {
            object.add(property.getKey().toString(), Json.value(property.getValue().toString()));
        }
        return object;
    }

    @Override
    public void fromJson(JsonObject json) {
        mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(getBoolean(json, "enabled"));
        mapStoreConfig.setWriteBatchSize(getInt(json, "writeBatchSize"));
        mapStoreConfig.setWriteDelaySeconds(getInt(json, "writeDelaySeconds"));
        mapStoreConfig.setWriteCoalescing(getBoolean(json, "writeCoalescing"));
        mapStoreConfig.setInitialLoadMode(InitialLoadMode.valueOf(getString(json, "initialLoadMode")));
        mapStoreConfig.setProperties(fromJsonObject(json));

        JsonValue className = json.get("className");
        if (className != null && !className.isNull()) {
            mapStoreConfig.setClassName(getString(json, "className"));
        }

        JsonValue factoryClassName = json.get("factoryClassName");
        if (factoryClassName != null && !factoryClassName.isNull()) {
            mapStoreConfig.setFactoryClassName(getString(json, "factoryClassName"));
        }
    }

    private static Properties fromJsonObject(JsonObject json) {
        Properties properties = new Properties();
        JsonObject jsonObject = getObject(json, "properties");
        for (JsonObject.Member property : jsonObject) {
            properties.put(property.getName(), property.getValue().asString());
        }
        return properties;
    }

    public MapStoreConfig getConfig() {
        return mapStoreConfig;
    }
}
