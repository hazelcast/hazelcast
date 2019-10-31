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

import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.json.JsonSerializable;

import static com.hazelcast.internal.util.JsonUtil.getBoolean;
import static com.hazelcast.internal.util.JsonUtil.getInt;
import static com.hazelcast.internal.util.JsonUtil.getString;

class NearCachePreloaderConfigDTO implements JsonSerializable {
    private NearCachePreloaderConfig preloaderConfig;

    NearCachePreloaderConfigDTO() {
    }

    NearCachePreloaderConfigDTO(NearCachePreloaderConfig preloaderConfig) {
        this.preloaderConfig = preloaderConfig;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject()
                .add("enabled", preloaderConfig.isEnabled())
                .add("directory", preloaderConfig.getDirectory())
                .add("storeInitialDelaySeconds", preloaderConfig.getStoreInitialDelaySeconds())
                .add("storeIntervalSeconds", preloaderConfig.getStoreIntervalSeconds());
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        preloaderConfig = new NearCachePreloaderConfig();
        preloaderConfig.setEnabled(getBoolean(json, "enabled"));
        preloaderConfig.setDirectory(getString(json, "directory"));
        preloaderConfig.setStoreInitialDelaySeconds(getInt(json, "storeInitialDelaySeconds"));
        preloaderConfig.setStoreIntervalSeconds(getInt(json, "storeIntervalSeconds"));
    }

    public NearCachePreloaderConfig getConfig() {
        return preloaderConfig;
    }
}
