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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.internal.config.ConfigNamespace;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;

import java.util.Set;

import static java.util.Collections.emptySet;

/**
 * Result of the config update operation.
 */
public class ConfigUpdateResult {

    public static final ConfigUpdateResult EMPTY = new ConfigUpdateResult(emptySet(), emptySet());
    /**
     * Newly added configs.
     */
    private final Set<ConfigNamespace> addedConfigs;
    /**
     * Ignored configs, because their equal exist already.
     */
    private final Set<ConfigNamespace> ignoredConfigs;

    public ConfigUpdateResult(Set<ConfigNamespace> addedConfigs, Set<ConfigNamespace> ignoredConfigs) {
        this.addedConfigs = addedConfigs;
        this.ignoredConfigs = ignoredConfigs;
    }

    public Set<ConfigNamespace> getAddedConfigs() {
        return addedConfigs;
    }

    public Set<ConfigNamespace> getIgnoredConfigs() {
        return ignoredConfigs;
    }

    public JsonObject toJson() {
        JsonObject json = new JsonObject();

        JsonArray addedConfigsAsJson = toJsonArray(addedConfigs);
        JsonArray ignoredConfigsAsJson = toJsonArray(ignoredConfigs);

        json.add("addedConfigs", addedConfigsAsJson);
        json.add("ignoredConfigs", ignoredConfigsAsJson);

        return json;
    }

    private JsonArray toJsonArray(Set<ConfigNamespace> configs) {
        JsonArray configsAsJson = new JsonArray();
        for (ConfigNamespace ns : configs) {
            JsonObject namespaceAsJson = new JsonObject();
            namespaceAsJson.add("sectionName", ns.getSectionName());
            namespaceAsJson.add("configName", ns.getConfigName());
            configsAsJson.add(namespaceAsJson);
        }
        return configsAsJson;
    }
}
