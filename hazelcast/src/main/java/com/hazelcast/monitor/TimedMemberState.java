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

package com.hazelcast.monitor;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.management.JsonSerializable;
import com.hazelcast.monitor.impl.MemberStateImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.hazelcast.util.JsonUtil.getArray;
import static com.hazelcast.util.JsonUtil.getBoolean;
import static com.hazelcast.util.JsonUtil.getLong;
import static com.hazelcast.util.JsonUtil.getObject;
import static com.hazelcast.util.JsonUtil.getString;
import static com.hazelcast.util.SetUtil.createHashSet;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;

public final class TimedMemberState implements Cloneable, JsonSerializable {

    long time;
    MemberStateImpl memberState;
    Set<String> instanceNames;
    List<String> memberList;
    boolean master;
    String clusterName;
    boolean sslEnabled;
    boolean lite;
    boolean socketInterceptorEnabled;

    public List<String> getMemberList() {
        return memberList;
    }

    public void setMemberList(List<String> memberList) {
        this.memberList = memberList;
    }

    public boolean isMaster() {
        return master;
    }

    public void setMaster(boolean master) {
        this.master = master;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getTime() {
        return time;
    }

    public Set<String> getInstanceNames() {
        return instanceNames;
    }

    public void setInstanceNames(Set<String> longInstanceNames) {
        this.instanceNames = longInstanceNames;
    }

    public MemberStateImpl getMemberState() {
        return memberState;
    }

    public void setMemberState(MemberStateImpl memberState) {
        this.memberState = memberState;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    public boolean isLite() {
        return lite;
    }

    public void setLite(boolean lite) {
        this.lite = lite;
    }

    public boolean isSocketInterceptorEnabled() {
        return socketInterceptorEnabled;
    }

    public void setSocketInterceptorEnabled(boolean socketInterceptorEnabled) {
        this.socketInterceptorEnabled = socketInterceptorEnabled;
    }

    @Override
    public TimedMemberState clone() throws CloneNotSupportedException {
        TimedMemberState state = (TimedMemberState) super.clone();
        state.setTime(time);
        state.setMemberState(memberState);
        state.setInstanceNames(instanceNames);
        state.setMemberList(memberList);
        state.setMaster(master);
        state.setClusterName(clusterName);
        state.setSslEnabled(sslEnabled);
        state.setLite(lite);
        state.setSocketInterceptorEnabled(socketInterceptorEnabled);
        return state;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("master", master);
        root.add("time", time);
        root.add("clusterName", clusterName);
        JsonArray instanceNames = new JsonArray();
        for (String instanceName : this.instanceNames) {
            instanceNames.add(instanceName);
        }
        root.add("instanceNames", instanceNames);
        if (memberList != null) {
            JsonArray members = new JsonArray();
            for (String member : memberList) {
                members.add(member);
            }
            root.add("memberList", members);
        }
        root.add("memberState", memberState.toJson());
        root.add("sslEnabled", sslEnabled);
        if (memberState.getNodeState().getClusterVersion().isGreaterOrEqual(Versions.V3_9)) {
            root.add("lite", lite);
        }
        if (memberState.getNodeState().getClusterVersion().isGreaterOrEqual(Versions.V3_9)) {
            root.add("socketInterceptorEnabled", socketInterceptorEnabled);
        }
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        time = getLong(json, "time");
        master = getBoolean(json, "master");
        clusterName = getString(json, "clusterName");
        final JsonArray jsonInstanceNames = getArray(json, "instanceNames");
        instanceNames = createHashSet(jsonInstanceNames.size());
        for (JsonValue instanceName : jsonInstanceNames.values()) {
            instanceNames.add(instanceName.asString());
        }
        final JsonArray jsonMemberList = getArray(json, "memberList");
        memberList = new ArrayList<String>(jsonMemberList.size());
        for (JsonValue member : jsonMemberList.values()) {
            memberList.add(member.asString());
        }
        final JsonObject jsonMemberState = getObject(json, "memberState");
        memberState = new MemberStateImpl();
        memberState.fromJson(jsonMemberState);
        sslEnabled = getBoolean(json, "sslEnabled", false);
        if (memberState.getNodeState().getClusterVersion().isGreaterOrEqual(Versions.V3_9)) {
            lite = getBoolean(json, "lite");
        }
        if (memberState.getNodeState().getClusterVersion().isGreaterOrEqual(Versions.V3_9)) {
            socketInterceptorEnabled = getBoolean(json, "socketInterceptorEnabled");
        }
    }

    @Override
    public String toString() {
        return "TimedMemberState{"
                + LINE_SEPARATOR + '\t' + memberState
                + LINE_SEPARATOR + "} Instances: " + instanceNames;
    }
}
