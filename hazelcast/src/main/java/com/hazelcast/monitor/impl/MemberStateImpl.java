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

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.monitor.HotRestartState;
import com.hazelcast.monitor.LocalOperationStats;
import com.hazelcast.monitor.MemberPartitionState;
import com.hazelcast.monitor.MemberState;
import com.hazelcast.monitor.NodeState;

import static com.hazelcast.util.JsonUtil.getObject;
import static com.hazelcast.util.JsonUtil.getString;

public class MemberStateImpl implements MemberState {

    private String address;
    private MemberPartitionState memberPartitionState = new MemberPartitionStateImpl();
    private LocalOperationStats operationStats = new LocalOperationStatsImpl();
    private HotRestartState hotRestartState = new HotRestartStateImpl();
    private NodeState nodeState = new NodeStateImpl();

    public MemberStateImpl() {
    }

    @Override
    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public LocalOperationStats getOperationStats() {
        return operationStats;
    }

    public void setOperationStats(LocalOperationStats operationStats) {
        this.operationStats = operationStats;
    }

    @Override
    public MemberPartitionState getMemberPartitionState() {
        return memberPartitionState;
    }

    @Override
    public HotRestartState getHotRestartState() {
        return hotRestartState;
    }

    public void setHotRestartState(HotRestartState hotRestartState) {
        this.hotRestartState = hotRestartState;
    }

    @Override
    public NodeState getNodeState() {
        return nodeState;
    }

    public void setNodeState(NodeState nodeState) {
        this.nodeState = nodeState;
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        root.add("address", address);
        root.add("operationStats", operationStats.toJson());
        root.add("memberPartitionState", memberPartitionState.toJson());
        root.add("nodeState", nodeState.toJson());
        root.add("hotRestartState", hotRestartState.toJson());
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        address = getString(json, "address");
        JsonObject jsonOperationStats = getObject(json, "operationStats", null);
        if (jsonOperationStats != null) {
            operationStats.fromJson(jsonOperationStats);
        }
        JsonObject jsonMemberPartitionState = getObject(json, "memberPartitionState", null);
        if (jsonMemberPartitionState != null) {
            memberPartitionState = new MemberPartitionStateImpl();
            memberPartitionState.fromJson(jsonMemberPartitionState);
        }
        JsonObject jsonNodeState = getObject(json, "nodeState", null);
        if (jsonNodeState != null) {
            nodeState = new NodeStateImpl();
            nodeState.fromJson(jsonNodeState);
        }
        JsonObject jsonHotRestartState = getObject(json, "hotRestartState", null);
        if (jsonHotRestartState != null) {
            hotRestartState = new HotRestartStateImpl();
            hotRestartState.fromJson(jsonHotRestartState);
        }
    }

    @Override
    public String toString() {
        return "MemberStateImpl{"
                + "address=" + address
                + ", operationStats=" + operationStats
                + ", memberPartitionState=" + memberPartitionState
                + ", nodeState=" + nodeState
                + ", hotRestartState=" + hotRestartState
                + '}';
    }

}
