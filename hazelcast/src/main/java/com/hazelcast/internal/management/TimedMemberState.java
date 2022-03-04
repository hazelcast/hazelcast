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

package com.hazelcast.internal.management;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.monitor.MemberState;
import com.hazelcast.internal.monitor.impl.MemberStateImpl;
import com.hazelcast.json.internal.JsonSerializable;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.util.JsonUtil.getArray;
import static com.hazelcast.internal.util.JsonUtil.getBoolean;
import static com.hazelcast.internal.util.JsonUtil.getLong;
import static com.hazelcast.internal.util.JsonUtil.getObject;
import static com.hazelcast.internal.util.JsonUtil.getString;
import static com.hazelcast.internal.util.StringUtil.LINE_SEPARATOR;

/**
 * Container for a {@link MemberState} with a timestamp.
 */
public final class TimedMemberState implements Cloneable, JsonSerializable {

    long time;
    MemberStateImpl memberState;
    List<String> memberList;
    boolean master;
    String clusterName;
    boolean sslEnabled;
    boolean lite;
    boolean socketInterceptorEnabled;
    boolean scriptingEnabled;
    boolean consoleEnabled;
    boolean mcDataAccessEnabled;

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

    public boolean isScriptingEnabled() {
        return scriptingEnabled;
    }

    public void setScriptingEnabled(boolean scriptingEnabled) {
        this.scriptingEnabled = scriptingEnabled;
    }

    public boolean isConsoleEnabled() {
        return consoleEnabled;
    }

    public void setConsoleEnabled(boolean consoleEnabled) {
        this.consoleEnabled = consoleEnabled;
    }

    public boolean isMcDataAccessEnabled() {
        return mcDataAccessEnabled;
    }

    public void setMcDataAccessEnabled(boolean mcDataAccessEnabled) {
        this.mcDataAccessEnabled = mcDataAccessEnabled;
    }

    @Override
    public TimedMemberState clone() throws CloneNotSupportedException {
        TimedMemberState state = (TimedMemberState) super.clone();
        state.setTime(time);
        state.setMemberState(memberState);
        state.setMemberList(memberList);
        state.setMaster(master);
        state.setClusterName(clusterName);
        state.setSslEnabled(sslEnabled);
        state.setLite(lite);
        state.setSocketInterceptorEnabled(socketInterceptorEnabled);
        state.setScriptingEnabled(scriptingEnabled);
        state.setConsoleEnabled(consoleEnabled);
        state.setMcDataAccessEnabled(mcDataAccessEnabled);
        return state;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("master", master);
        root.add("time", time);
        root.add("clusterName", clusterName);
        if (memberList != null) {
            JsonArray members = new JsonArray();
            for (String member : memberList) {
                members.add(member);
            }
            root.add("memberList", members);
        }
        root.add("memberState", memberState.toJson());
        root.add("sslEnabled", sslEnabled);
        root.add("lite", lite);
        root.add("socketInterceptorEnabled", socketInterceptorEnabled);
        root.add("scriptingEnabled", scriptingEnabled);
        root.add("consoleEnabled", consoleEnabled);
        root.add("mcDataAccessEnabled", mcDataAccessEnabled);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        time = getLong(json, "time");
        master = getBoolean(json, "master");
        clusterName = getString(json, "clusterName");
        JsonArray jsonMemberList = getArray(json, "memberList");
        memberList = new ArrayList<String>(jsonMemberList.size());
        for (JsonValue member : jsonMemberList.values()) {
            memberList.add(member.asString());
        }
        JsonObject jsonMemberState = getObject(json, "memberState");
        memberState = new MemberStateImpl();
        memberState.fromJson(jsonMemberState);
        sslEnabled = getBoolean(json, "sslEnabled", false);
        lite = getBoolean(json, "lite");
        socketInterceptorEnabled = getBoolean(json, "socketInterceptorEnabled");
        scriptingEnabled = getBoolean(json, "scriptingEnabled");
        consoleEnabled = getBoolean(json, "consoleEnabled");
        mcDataAccessEnabled = getBoolean(json, "mcDataAccessEnabled");
    }

    @Override
    public String toString() {
        return "TimedMemberState{" + LINE_SEPARATOR + '\t' + memberState + LINE_SEPARATOR + "}";
    }
}
