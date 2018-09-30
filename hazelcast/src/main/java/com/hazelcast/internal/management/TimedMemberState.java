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

package com.hazelcast.internal.management;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.monitor.MemberState;
import com.hazelcast.monitor.impl.MemberStateImpl;

import static com.hazelcast.util.JsonUtil.getBoolean;
import static com.hazelcast.util.JsonUtil.getLong;
import static com.hazelcast.util.JsonUtil.getObject;
import static com.hazelcast.util.JsonUtil.getString;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;

/**
 * Container for a {@link MemberState} with a timestamp.
 */
public final class TimedMemberState implements Cloneable, JsonSerializable {

    long time;
    MemberStateImpl memberState;
    boolean master;
    String clusterName;
    boolean socketInterceptorEnabled;

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
        state.setClusterName(clusterName);
        state.setSocketInterceptorEnabled(socketInterceptorEnabled);
        return state;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("time", time);
        root.add("clusterName", clusterName);
        root.add("memberState", memberState.toJson());
        root.add("socketInterceptorEnabled", socketInterceptorEnabled);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        time = getLong(json, "time");
        clusterName = getString(json, "clusterName");
        JsonObject jsonMemberState = getObject(json, "memberState");
        memberState = new MemberStateImpl();
        memberState.fromJson(jsonMemberState);
        socketInterceptorEnabled = getBoolean(json, "socketInterceptorEnabled");
    }

    @Override
    public String toString() {
        return "TimedMemberState{" + LINE_SEPARATOR + '\t' + memberState + LINE_SEPARATOR + "}";
    }
}
