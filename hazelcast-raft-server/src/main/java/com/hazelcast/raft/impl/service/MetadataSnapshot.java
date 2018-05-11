package com.hazelcast.raft.impl.service;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftMemberImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 *
 */
public final class MetadataSnapshot implements IdentifiedDataSerializable {

    private final Collection<RaftMemberImpl> members = new ArrayList<RaftMemberImpl>();
    private final Collection<RaftGroupInfo> raftGroups = new ArrayList<RaftGroupInfo>();
    private MembershipChangeContext membershipChangeContext;

    public void addRaftGroup(RaftGroupInfo group) {
        raftGroups.add(group);
    }

    public void addMember(RaftMemberImpl member) {
        members.add(member);
    }

    public Collection<RaftMemberImpl> getMembers() {
        return members;
    }

    public Collection<RaftGroupInfo> getRaftGroups() {
        return raftGroups;
    }

    public MembershipChangeContext getMembershipChangeContext() {
        return membershipChangeContext;
    }

    public void setMembershipChangeContext(MembershipChangeContext membershipChangeContext) {
        this.membershipChangeContext = membershipChangeContext;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.METADATA_SNAPSHOT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(members.size());
        for (RaftMemberImpl member : members) {
            out.writeObject(member);
        }
        out.writeInt(raftGroups.size());
        for (RaftGroupInfo group : raftGroups) {
            out.writeObject(group);
        }
        out.writeObject(membershipChangeContext);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        for (int i = 0; i < len; i++) {
            RaftMemberImpl member = in.readObject();
            members.add(member);
        }

        len = in.readInt();
        for (int i = 0; i < len; i++) {
            RaftGroupInfo group = in.readObject();
            raftGroups.add(group);
        }
        membershipChangeContext = in.readObject();
    }
}
