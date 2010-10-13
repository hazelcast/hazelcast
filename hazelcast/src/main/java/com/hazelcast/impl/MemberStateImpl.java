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

package com.hazelcast.impl;

import com.hazelcast.core.Member;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.monitor.MemberState;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MemberStateImpl implements MemberState {
    /**
     *
     */
    private static final long serialVersionUID = -1817978625085375340L;
    Member member;
    Map<String, LocalMapStatsImpl> mapStats = new HashMap<String, LocalMapStatsImpl>();
    Map<String, LocalQueueStatsImpl> queueStats = new HashMap<String, LocalQueueStatsImpl>();

    public void writeData(DataOutput out) throws IOException {
        member.writeData(out);
        int mapCount = mapStats.size();
        int queueCount = queueStats.size();
        out.writeInt(mapCount);
        Set<Map.Entry<String, LocalMapStatsImpl>> maps = mapStats.entrySet();
        for (Map.Entry<String, LocalMapStatsImpl> mapStatsEntry : maps) {
            out.writeUTF(mapStatsEntry.getKey());
            mapStatsEntry.getValue().writeData(out);
        }
        out.writeInt(queueCount);
        Set<Map.Entry<String, LocalQueueStatsImpl>> queueStatEntries = queueStats.entrySet();
        for (Map.Entry<String, LocalQueueStatsImpl> queueStatEntry : queueStatEntries) {
            out.writeUTF(queueStatEntry.getKey());
            queueStatEntry.getValue().writeData(out);
        }
    }

    public void readData(DataInput in) throws IOException {
        member = new MemberImpl();
        member.readData(in);
        int mapCount = in.readInt();
        for (int i = 0; i < mapCount; i++) {
            String mapName = in.readUTF();
            LocalMapStatsImpl localMapStatsImpl = new LocalMapStatsImpl();
            localMapStatsImpl.readData(in);
            mapStats.put(mapName, localMapStatsImpl);
        }
        int queueCount = in.readInt();
        for (int i = 0; i < queueCount; i++) {
            String queueName = in.readUTF();
            LocalQueueStatsImpl localQueueStats = new LocalQueueStatsImpl();
            localQueueStats.readData(in);
            queueStats.put(queueName, localQueueStats);
        }
    }

    public Member getMember() {
        return member;
    }

    public LocalMapStats getLocalMapStats(String mapName) {
        return mapStats.get(mapName);
    }

    public LocalQueueStats getLocalQueueStats(String queueName) {
        return queueStats.get(queueName);
    }

    public void setMember(Member member) {
        this.member = member;
    }

    public void putLocalMapStats(String mapName, LocalMapStatsImpl localMapStats) {
        mapStats.put(mapName, localMapStats);
    }

    public void putLocalQueueStats(String queueName, LocalQueueStatsImpl localQueueStats) {
        queueStats.put(queueName, localQueueStats);
    }

    @Override
    public String toString() {
        return "MemberStateImpl [" + member +
                "] { mapStats=" + mapStats +
                "\n queueStats=" + queueStats +
                '}';
    }
}