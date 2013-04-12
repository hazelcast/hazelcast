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

import com.hazelcast.monitor.impl.MemberStateImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TimedMemberState implements DataSerializable, Cloneable {
    long time;
    MemberState memberState = null;
    Set<String> instanceNames = null;
    List<String> memberList;
    Boolean master;
    String clusterName;

    public TimedMemberState clone() {
        TimedMemberState st = new TimedMemberState();
        st.setTime(time);
        st.setMemberState(memberState);
        st.setInstanceNames(instanceNames);
        st.setMemberList(memberList);
        st.setMaster(master);
        st.setClusterName(clusterName);
        return st;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(time);
        out.writeBoolean(master);
        memberState.writeData(out);
        out.writeUTF(clusterName);
        int nameCount = (instanceNames == null) ? 0 : instanceNames.size();
        out.writeInt(nameCount);
        if (instanceNames != null) {
            for (String name : instanceNames) {
                out.writeUTF(name);
            }
        }
        int memberCount = (memberList == null) ? 0 : memberList.size();
        out.writeInt(memberCount);
        if (memberList != null) {
            for (String address : memberList) {
                out.writeUTF(address);
            }
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        time = in.readLong();
        master = in.readBoolean();
        memberState = new MemberStateImpl();
        memberState.readData(in);
        clusterName = in.readUTF();
        int nameCount = in.readInt();
        instanceNames = new HashSet<String>(nameCount);
        for (int i = 0; i < nameCount; i++) {
            instanceNames.add(in.readUTF());
        }
        int memberCount = in.readInt();
        memberList = new ArrayList<String>();
        for (int i = 0; i < memberCount; i++) {
            memberList.add(in.readUTF());
        }
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

    public MemberState getMemberState() {
        return memberState;
    }

    public void setMemberState(MemberState memberState) {
        this.memberState = memberState;
    }
}
