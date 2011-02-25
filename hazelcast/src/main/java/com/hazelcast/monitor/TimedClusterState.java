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

import com.hazelcast.impl.MemberStateImpl;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TimedClusterState implements DataSerializable {
    long time;
    List<MemberState> lsMemberStates = new ArrayList<MemberState>(100);
    Set<String> instanceNames = null;

    public void writeData(DataOutput out) throws IOException {
        out.writeLong(time);
        out.writeInt(lsMemberStates.size());
        for (MemberState memberState : lsMemberStates) {
            memberState.writeData(out);
        }
        int nameCount = (instanceNames == null) ? 0 : instanceNames.size();
        out.writeInt(nameCount);
        if (instanceNames != null) {
            for (String name : instanceNames) {
                out.writeUTF(name);
            }
        }
    }

    public void readData(DataInput in) throws IOException {
        time = in.readLong();
        int memberStatsCount = in.readInt();
        for (int i = 0; i < memberStatsCount; i++) {
            MemberStateImpl memberState = new MemberStateImpl();
            memberState.readData(in);
            lsMemberStates.add(memberState);
        }
        int nameCount = in.readInt();
        instanceNames = new HashSet<String>(nameCount);
        for (int i = 0; i < nameCount; i++) {
            instanceNames.add(in.readUTF());
        }
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

    public List<MemberState> getMemberStates() {
        return lsMemberStates;
    }

    public void addMemberState(MemberState memberState) {
        lsMemberStates.add(memberState);
    }

    public void setInstanceNames(Set<String> longInstanceNames) {
        this.instanceNames = longInstanceNames;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("TimedClusterState{\n");
        for (MemberState memberState : lsMemberStates) {
            sb.append("\t");
            sb.append(memberState);
            sb.append("\n");
        }
        sb.append("}\n");
        sb.append("Instances : ");
        sb.append(instanceNames);
        return sb.toString();
    }
}

