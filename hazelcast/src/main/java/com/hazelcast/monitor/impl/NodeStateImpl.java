/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.eclipsesource.json.JsonObject;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.monitor.NodeState;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;

import static com.hazelcast.util.JsonUtil.getString;

public class NodeStateImpl implements NodeState {

    private ClusterState clusterState;
    private com.hazelcast.instance.NodeState nodeState;

    private Version clusterVersion;
    private MemberVersion memberVersion;

    public NodeStateImpl() {
    }

    public NodeStateImpl(ClusterState clusterState, com.hazelcast.instance.NodeState nodeState,
                         Version clusterVersion, MemberVersion memberVersion) {
        this.clusterState = clusterState;
        this.nodeState = nodeState;
        this.clusterVersion = clusterVersion;
        this.memberVersion = memberVersion;
    }

    @Override
    public ClusterState getClusterState() {
        return clusterState;
    }

    @Override
    public com.hazelcast.instance.NodeState getNodeState() {
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
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        String jsonClusterState = getString(json, "clusterState", null);
        if (jsonClusterState != null) {
            clusterState = ClusterState.valueOf(jsonClusterState);
        }
        String jsonNodeState = getString(json, "nodeState", null);
        if (jsonNodeState != null) {
            nodeState = com.hazelcast.instance.NodeState.valueOf(jsonNodeState);
        }
        String jsonClusterVersion = getString(json, "clusterVersion", null);
        if (jsonClusterVersion != null) {
            clusterVersion = Version.of(jsonClusterVersion);
        }
        String jsonNodeVersion = getString(json, "memberVersion", null);
        if (jsonNodeState != null) {
            memberVersion = MemberVersion.of(jsonNodeVersion);
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
