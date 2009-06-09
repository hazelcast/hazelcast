/**
 * 
 */
package com.hazelcast.impl.cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.hazelcast.impl.MemberImpl;

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
        ClusterManager.get().updateMembers(lsMemberInfos);
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