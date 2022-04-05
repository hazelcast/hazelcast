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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.monitor.NodeState;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.JsonUtil.getString;

public class NodeStateImpl implements NodeState {

    private ClusterState clusterState;
    private com.hazelcast.instance.impl.NodeState nodeState;

    private Version clusterVersion;
    private MemberVersion memberVersion;

    private Map<String, List<String>> weakSecretsConfigs;

    public NodeStateImpl() {
    }

    public NodeStateImpl(ClusterState clusterState, com.hazelcast.instance.impl.NodeState nodeState,
                         Version clusterVersion, MemberVersion memberVersion) {
        this(clusterState, nodeState, clusterVersion, memberVersion, Collections.<String, List<String>>emptyMap());
    }

    public NodeStateImpl(ClusterState clusterState, com.hazelcast.instance.impl.NodeState nodeState,
                         Version clusterVersion, MemberVersion memberVersion, Map<String,
                         List<String>> weakSecretsConfigs) {
        this.clusterState = clusterState;
        this.nodeState = nodeState;
        this.clusterVersion = clusterVersion;
        this.memberVersion = memberVersion;
        this.weakSecretsConfigs = weakSecretsConfigs;
    }

    @Override
    public ClusterState getClusterState() {
        return clusterState;
    }

    @Override
    public com.hazelcast.instance.impl.NodeState getNodeState() {
        return nodeState;
    }

    @Override
    public Version getClusterVersion() {
        return clusterVersion;
    }

    public MemberVersion getMemberVersion() {
        return memberVersion;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("clusterState", clusterState.name());
        root.add("nodeState", nodeState.name());
        root.add("clusterVersion", clusterVersion.toString());
        root.add("memberVersion", memberVersion.toString());

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
        String jsonClusterState = getString(json, "clusterState", null);
        if (jsonClusterState != null) {
            clusterState = ClusterState.valueOf(jsonClusterState);
        }
        String jsonNodeState = getString(json, "nodeState", null);
        if (jsonNodeState != null) {
            nodeState = com.hazelcast.instance.impl.NodeState.valueOf(jsonNodeState);
        }
        String jsonClusterVersion = getString(json, "clusterVersion", null);
        if (jsonClusterVersion != null) {
            clusterVersion = Version.of(jsonClusterVersion);
        }
        String jsonNodeVersion = getString(json, "memberVersion", null);
        if (jsonNodeState != null) {
            memberVersion = MemberVersion.of(jsonNodeVersion);
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
                + "clusterState=" + clusterState
                + ", nodeState=" + nodeState
                + ", clusterVersion=" + clusterVersion
                + ", memberVersion=" + memberVersion
                + '}';
    }

}
