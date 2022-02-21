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

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.json.internal.JsonSerializable;

import static com.hazelcast.internal.util.JsonUtil.getInt;
import static com.hazelcast.internal.util.JsonUtil.getString;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

class EvictionConfigDTO implements JsonSerializable {
    private EvictionConfig evictionConfig;

    EvictionConfigDTO() {
    }

    EvictionConfigDTO(EvictionConfig evictionConfig) {
        this.evictionConfig = evictionConfig;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject()
                .add("size", evictionConfig.getSize())
                .add("maxSizePolicy", evictionConfig.getMaxSizePolicy().toString())
                .add("evictionPolicy", evictionConfig.getEvictionPolicy().toString());

        String comparatorClassName = evictionConfig.getComparatorClassName();
        if (!isNullOrEmpty(comparatorClassName)) {
            root.add("comparatorClassName", comparatorClassName);
        }
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        evictionConfig = new EvictionConfig();
        evictionConfig.setSize(getInt(json, "size"));
        evictionConfig.setMaxSizePolicy(MaxSizePolicy.valueOf(getString(json, "maxSizePolicy")));
        evictionConfig.setEvictionPolicy(EvictionPolicy.valueOf(getString(json, "evictionPolicy")));

        JsonValue comparatorClassName = json.get("comparatorClassName");
        if (comparatorClassName != null && !comparatorClassName.isNull()) {
            evictionConfig.setComparatorClassName(comparatorClassName.asString());
        }
    }

    public EvictionConfig getConfig() {
        return evictionConfig;
    }
}
