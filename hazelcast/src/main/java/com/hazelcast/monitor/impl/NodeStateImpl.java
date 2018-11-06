/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.monitor.impl;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.monitor.NodeState;
import com.hazelcast.version.Version;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.util.JsonUtil.getString;

public class NodeStateImpl implements NodeState {

    private Version clusterVersion;

    private Map<String, List<String>> weakSecretsConfigs;

    public NodeStateImpl() {
        this(null, Collections.<String, List<String>>emptyMap());
    }

    public NodeStateImpl(Version clusterVersion, Map<String, List<String>> weakSecretsConfigs) {
        this.clusterVersion = clusterVersion;
        this.weakSecretsConfigs = weakSecretsConfigs;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("clusterVersion", clusterVersion.toString());

        JsonObject weaknesses = new JsonObject();
        for (Map.Entry<String, List<String>> entry : weakSecretsConfigs.entrySet()) {
            JsonArray values = new JsonArray();
            for (String value : entry.getValue()) {
                values.add(value);
            }
            weaknesses.add(entry.getKey(), values);
        }
        root.add("weakConfigs", weaknesses);
        return root;
    }

    @SuppressWarnings({"checkstyle:npathcomplexity"})
    @Override
    public void fromJson(JsonObject json) {
        String jsonClusterVersion = getString(json, "clusterVersion", null);
        if (jsonClusterVersion != null) {
            clusterVersion = Version.of(jsonClusterVersion);
        }
        weakSecretsConfigs = new HashMap<String, List<String>>();
        JsonValue jsonWeakConfigs = json.get("weakConfigs");
        if (jsonWeakConfigs != null) {
            JsonObject weakConfigsJsObj = jsonWeakConfigs.asObject();
            for (JsonObject.Member member : weakConfigsJsObj) {
                List<String> weaknesses = new ArrayList<String>();
                for (JsonValue value : member.getValue().asArray()) {
                    weaknesses.add(value.asString());
                }
                weakSecretsConfigs.put(member.getName(), weaknesses);
            }
        }
    }

    @Override
    public String toString() {
        return "NodeStateImpl{"
                + "clusterVersion=" + clusterVersion
                + '}';
    }

}
