/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.management.JsonSerializable;
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

public final class TimedMemberState implements Cloneable, JsonSerializable {

    long time;
    MemberStateImpl memberState;
    Set<String> instanceNames;
    List<String> memberList;
    Boolean master;
    String clusterName;

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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TimedMemberState that = (TimedMemberState) o;

        if (time != that.time) {
            return false;
        }
        if (clusterName != null ? !clusterName.equals(that.clusterName) : that.clusterName != null) {
            return false;
        }
        if (instanceNames != null ? !instanceNames.equals(that.instanceNames) : that.instanceNames != null) {
            return false;
        }
        if (master != null ? !master.equals(that.master) : that.master != null) {
            return false;
        }
        if (memberList != null ? !memberList.equals(that.memberList) : that.memberList != null) {
            return false;
        }
        if (memberState != null ? !memberState.equals(that.memberState) : that.memberState != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (time ^ (time >>> 32));
        result = 31 * result + (memberState != null ? memberState.hashCode() : 0);
        result = 31 * result + (instanceNames != null ? instanceNames.hashCode() : 0);
        result = 31 * result + (memberList != null ? memberList.hashCode() : 0);
        result = 31 * result + (master != null ? master.hashCode() : 0);
        result = 31 * result + (clusterName != null ? clusterName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("TimedMemberState{\n");
        sb.append("\t");
        sb.append(memberState);
        sb.append("\n");
        sb.append("}\n");
        sb.append("Instances : ");
        sb.append(instanceNames);
        return sb.toString();
    }
}
