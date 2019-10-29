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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.json.JsonSerializable;

import static com.hazelcast.internal.util.JsonUtil.getBoolean;
import static com.hazelcast.internal.util.JsonUtil.getInt;
import static com.hazelcast.internal.util.JsonUtil.getObject;
import static com.hazelcast.internal.util.JsonUtil.getString;

class NearCacheConfigDTO implements JsonSerializable {
    private NearCacheConfig nearCacheConfig;

    NearCacheConfigDTO() {
    }

    NearCacheConfigDTO(NearCacheConfig nearCacheConfig) {
        this.nearCacheConfig = nearCacheConfig;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject()
                .add("name", nearCacheConfig.getName())
                .add("cacheLocalEntries", nearCacheConfig.isCacheLocalEntries())
                .add("serializeKeys", nearCacheConfig.isSerializeKeys())
                .add("invalidateOnChange", nearCacheConfig.isInvalidateOnChange())
                .add("timeToLiveSeconds", nearCacheConfig.getTimeToLiveSeconds())
                .add("maxIdleSeconds", nearCacheConfig.getMaxIdleSeconds())
                .add("inMemoryFormat", nearCacheConfig.getInMemoryFormat().toString())
                .add("localUpdatePolicy", nearCacheConfig.getLocalUpdatePolicy().toString());

        EvictionConfigDTO evictionConfigDTO
                = new EvictionConfigDTO(nearCacheConfig.getEvictionConfig());
        root.add("evictionConfig", evictionConfigDTO.toJson());

        NearCachePreloaderConfigDTO preloaderConfigDTO
                = new NearCachePreloaderConfigDTO(nearCacheConfig.getPreloaderConfig());
        root.add("preloaderConfig", preloaderConfigDTO.toJson());
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setName(getString(json, "name"));
        nearCacheConfig.setCacheLocalEntries(getBoolean(json, "cacheLocalEntries"));
        nearCacheConfig.setSerializeKeys(getBoolean(json, "serializeKeys"));
        nearCacheConfig.setInvalidateOnChange(getBoolean(json, "invalidateOnChange"));
        nearCacheConfig.setTimeToLiveSeconds(getInt(json, "timeToLiveSeconds"));
        nearCacheConfig.setMaxIdleSeconds(getInt(json, "maxIdleSeconds"));
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.valueOf(getString(json, "inMemoryFormat")));
        nearCacheConfig.setLocalUpdatePolicy(LocalUpdatePolicy.valueOf(getString(json, "localUpdatePolicy")));

        EvictionConfigDTO evictionConfigDTO = new EvictionConfigDTO();
        evictionConfigDTO.fromJson(getObject(json, "evictionConfig"));
        nearCacheConfig.setEvictionConfig(evictionConfigDTO.getConfig());

        NearCachePreloaderConfigDTO preloaderConfigDTO = new NearCachePreloaderConfigDTO();
        preloaderConfigDTO.fromJson(getObject(json, "preloaderConfig"));
        nearCacheConfig.setPreloaderConfig(preloaderConfigDTO.getConfig());
    }

    public NearCacheConfig getConfig() {
        return nearCacheConfig;
    }
}
