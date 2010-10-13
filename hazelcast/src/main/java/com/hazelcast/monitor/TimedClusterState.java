/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.monitor;

import com.hazelcast.core.Member;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.impl.MemberStateImpl;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TimedClusterState implements DataSerializable {
    long time;
    Map<Member, MemberState> memberStates = new ConcurrentHashMap<Member, MemberState>();
    ClusterStateView clusterStateView;

    public void writeData(DataOutput out) throws IOException {
        out.writeLong(time);
        out.writeInt(memberStates.size());
        Set<Map.Entry<Member, MemberState>> memberStateEntries = memberStates.entrySet();
        for (Map.Entry<Member, MemberState> memberStatEntry : memberStateEntries) {
            memberStatEntry.getKey().writeData(out);
            memberStatEntry.getValue().writeData(out);
        }
        if (clusterStateView != null) {
            out.writeBoolean(true);
            clusterStateView.writeData(out);
        } else {
            out.writeBoolean(false);
        }
    }

    public void readData(DataInput in) throws IOException {
        time = in.readLong();
        int memberStatsCount = in.readInt();
        for (int i = 0; i < memberStatsCount; i++) {
            Member member = new MemberImpl();
            member.readData(in);
            MemberStateImpl memberStateImpl = new MemberStateImpl();
            memberStateImpl.readData(in);
            memberStates.put(member, memberStateImpl);
        }
        if (in.readBoolean()) {
            clusterStateView = new ClusterStateViewImpl();
            clusterStateView.readData(in);
        }
    }

    public boolean containsKey(Member member) {
        return memberStates.containsKey(member);
    }

    public void putMemberState(Member member, MemberState mapStat) {
        memberStates.put(member, mapStat);
    }

    public Map<Member, MemberState> getMemberState() {
        return memberStates;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getTime() {
        return time;
    }

    public ClusterStateView getClusterStateView() {
        return clusterStateView;
    }

    public void setClusterStateView(ClusterStateView clusterStateView) {
        this.clusterStateView = clusterStateView;
    }
}

