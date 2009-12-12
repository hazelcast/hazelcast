/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.impl.MemberImpl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MembersUpdateCall extends AbstractRemotelyCallable<Boolean> {

    public List<MemberInfo> lsMemberInfos = null;

    public MembersUpdateCall() {
    }

    public MembersUpdateCall(List<MemberImpl> lsMembers) {
        int size = lsMembers.size();
        lsMemberInfos = new ArrayList<MemberInfo>(size);
        for (int i = 0; i < size; i++) {
            MemberImpl member = lsMembers.get(i);
            lsMemberInfos.add(new MemberInfo(member.getAddress(), member.getNodeType()));
        }
    }

    public Boolean call() {
        node.clusterManager.updateMembers(lsMemberInfos);
        return Boolean.TRUE;
    }

    public void addMemberInfo(MemberInfo address) {
        if (!lsMemberInfos.contains(address)) {
            lsMemberInfos.add(address);
        }
    }

    @Override
    public void readData(DataInput in) throws IOException {
        int size = in.readInt();
        lsMemberInfos = new ArrayList<MemberInfo>(size);
        for (int i = 0; i < size; i++) {
            MemberInfo memberInfo = new MemberInfo();
            memberInfo.readData(in);
            lsMemberInfos.add(memberInfo);
        }
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        int size = lsMemberInfos.size();
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            MemberInfo memberInfo = lsMemberInfos.get(i);
            memberInfo.writeData(out);
        }
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer("MembersUpdateCall {");
        for (MemberInfo address : lsMemberInfos) {
            sb.append("\n").append(address);
        }
        sb.append("\n}");
        return sb.toString();
    }
}
