/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.management.JsonSerializable;
import com.hazelcast.monitor.impl.MemberStateImpl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.util.JsonUtil.getArray;
import static com.hazelcast.util.JsonUtil.getBoolean;
import static com.hazelcast.util.JsonUtil.getLong;
import static com.hazelcast.util.JsonUtil.getObject;
import static com.hazelcast.util.JsonUtil.getString;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;

public final class TimedMemberState implements Cloneable, JsonSerializable {

    long time;
    MemberStateImpl memberState;
    Set<String> instanceNames;
    List<String> memberList;
    Boolean master;
    String clusterName;

    public List<String> getMemberList() {
        return memberList;
    }

    public void setMemberList(List<String> memberList) {
        this.memberList = memberList;
    }

    public Boolean getMaster() {
        return master;
    }

    public void setMaster(Boolean master) {
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

    @Override
    public TimedMemberState clone() throws CloneNotSupportedException {
        TimedMemberState state = (TimedMemberState) super.clone();
        state.setTime(time);
        state.setMemberState(memberState);
        state.setInstanceNames(instanceNames);
        state.setMemberList(memberList);
        state.setMaster(master);
        state.setClusterName(clusterName);
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
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        time = getLong(json, "time");
        master = getBoolean(json, "master");
        clusterName = getString(json, "clusterName");
        instanceNames = new HashSet<String>();
        final JsonArray jsonInstanceNames = getArray(json, "instanceNames");
        for (JsonValue instanceName : jsonInstanceNames.values()) {
            instanceNames.add(instanceName.asString());
        }
        memberList = new ArrayList<String>();
        final JsonArray jsonMemberList = getArray(json, "memberList");
        for (JsonValue member : jsonMemberList.values()) {
            memberList.add(member.asString());
        }
        final JsonObject jsonMemberState = getObject(json, "memberState");
        memberState = new MemberStateImpl();
        memberState.fromJson(jsonMemberState);
    }

    @Override
    public String toString() {
        return "TimedMemberState{"
                + LINE_SEPARATOR + '\t' + memberState
                + LINE_SEPARATOR + "} Instances: " + instanceNames;
    }
}
