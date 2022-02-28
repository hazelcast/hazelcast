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

package com.hazelcast.internal.management.dto;

import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.json.internal.JsonSerializable;

import java.util.function.Consumer;

import static com.hazelcast.internal.util.JsonUtil.fromJsonObject;
import static com.hazelcast.internal.util.JsonUtil.toJsonObject;
import static com.hazelcast.internal.util.MapUtil.isNullOrEmpty;

/**
 * A JSON representation of {@link WanCustomPublisherConfig}.
 */
public class CustomWanPublisherConfigDTO implements JsonSerializable {

    private WanCustomPublisherConfig config;

    public CustomWanPublisherConfigDTO() {
    }

    public CustomWanPublisherConfigDTO(WanCustomPublisherConfig config) {
        this.config = config;
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public JsonObject toJson() {
        JsonObject root = new JsonObject();

        root.add("publisherId", config.getPublisherId());
        root.add("className", config.getClassName());

        if (!isNullOrEmpty(config.getProperties())) {
            root.add("properties", toJsonObject(config.getProperties()));
        }
        return root;
    }

    @Override
    @SuppressWarnings({"checkstyle:methodlength", "checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public void fromJson(JsonObject json) {
        config = new WanCustomPublisherConfig();

        consumeIfExists(json, "publisherId", v -> config.setPublisherId(v.asString()));
        consumeIfExists(json, "className", v -> config.setClassName(v.asString()));

        config.setProperties(fromJsonObject((JsonObject) json.get("properties")));
    }

    private void consumeIfExists(JsonObject json, String attribute, Consumer<JsonValue> valueConsumer) {
        JsonValue value = json.get(attribute);
        if (value != null && !value.isNull()) {
            valueConsumer.accept(value);
        }
    }

    public WanCustomPublisherConfig getConfig() {
        return config;
    }
}
