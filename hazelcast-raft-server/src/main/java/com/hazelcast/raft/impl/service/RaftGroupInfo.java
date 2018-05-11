package com.hazelcast.raft.impl.service;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroup;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.impl.RaftMemberImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static com.hazelcast.raft.RaftGroup.RaftGroupStatus.ACTIVE;
import static com.hazelcast.raft.RaftGroup.RaftGroupStatus.DESTROYED;
import static com.hazelcast.raft.RaftGroup.RaftGroupStatus.DESTROYING;
import static com.hazelcast.util.Preconditions.checkState;

/**
 * TODO: Javadoc Pending...
 */
public final class RaftGroupInfo implements RaftGroup, IdentifiedDataSerializable {

    private RaftGroupId id;
    private Set<RaftMemberImpl> initialMembers;
    private Set<RaftMemberImpl> members;
    private long membersCommitIndex;

    // read outside of Raft
    private volatile RaftGroupStatus status;

    private transient RaftMemberImpl[] membersArray;

    public RaftGroupInfo() {
    }

    public RaftGroupInfo(RaftGroupId id, Collection<RaftMemberImpl> members) {
        this.id = id;
        this.status = ACTIVE;
        this.initialMembers = Collections.unmodifiableSet(new LinkedHashSet<RaftMemberImpl>(members));
        this.members = Collections.unmodifiableSet(new LinkedHashSet<RaftMemberImpl>(members));
        this.membersArray = members.toArray(new RaftMemberImpl[0]);
    }

    @Override
    public RaftGroupId id() {
        return id;
    }

    public String name() {
        return id.name();
    }

    public long commitIndex() {
        return id.commitIndex();
    }

    public int initialMemberCount() {
        return initialMembers.size();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<RaftMember> members() {
        return (Collection) members;
    }

    public Collection<RaftMemberImpl> memberImpls() {
        return members;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<RaftMember> initialMembers() {
        return (Collection) initialMembers;
    }

    public boolean containsMember(RaftMemberImpl member) {
        return members.contains(member);
    }

    public int memberCount() {
        return members.size();
    }

    @Override
    public RaftGroupStatus status() {
        return status;
    }

    public boolean setDestroying() {
        if (status == DESTROYED) {
            return false;
        }

        status = DESTROYING;
        return true;
    }

    public boolean setDestroyed() {
        checkState(status != ACTIVE, "Cannot destroy " + id + " because status is: " + status);
        return forceSetDestroyed();
    }

    public boolean forceSetDestroyed() {
        if (status == DESTROYED) {
            return false;
        }

        status = DESTROYED;
        return true;
    }

    public long getMembersCommitIndex() {
        return membersCommitIndex;
    }

    public boolean applyMembershipChange(RaftMemberImpl leaving, RaftMemberImpl joining,
                              long expectedMembersCommitIndex, long newMembersCommitIndex) {
        checkState(status == ACTIVE, "Cannot apply membership change of Leave: " + leaving
                + " and Join: " + joining + " since status is: " + status);
        if (membersCommitIndex != expectedMembersCommitIndex) {
            return false;
        }

        Set<RaftMemberImpl> m = new LinkedHashSet<RaftMemberImpl>(members);
        if (leaving != null) {
            boolean removed = m.remove(leaving);
            assert removed : leaving + " is not member of " + toString();
        }

        if (joining != null) {
            boolean added = m.add(joining);
            assert added : joining + " is already member of " + toString();
        }

        members = Collections.unmodifiableSet(m);
        membersCommitIndex = newMembersCommitIndex;
        membersArray = members.toArray(new RaftMemberImpl[0]);
        return true;
    }

    public RaftMemberImpl[] membersArray() {
        return membersArray;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(id);
        out.writeInt(initialMembers.size());
        for (RaftMemberImpl member : initialMembers) {
            out.writeObject(member);
        }
        out.writeLong(membersCommitIndex);
        out.writeInt(members.size());
        for (RaftMemberImpl member : members) {
            out.writeObject(member);
        }
        out.writeUTF(status.toString());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readObject();
        int initialMemberCount = in.readInt();
        Set<RaftMemberImpl> initialMembers = new LinkedHashSet<RaftMemberImpl>();
        for (int i = 0; i < initialMemberCount; i++) {
            RaftMemberImpl member = in.readObject();
            initialMembers.add(member);
        }
        this.initialMembers = Collections.unmodifiableSet(initialMembers);
        membersCommitIndex = in.readLong();
        int memberCount = in.readInt();
        members = new LinkedHashSet<RaftMemberImpl>(memberCount);
        for (int i = 0; i < memberCount; i++) {
            RaftMemberImpl member = in.readObject();
            members.add(member);
        }
        membersArray = members.toArray(new RaftMemberImpl[0]);
        members = Collections.unmodifiableSet(members);
        status = RaftGroupStatus.valueOf(in.readUTF());
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.RAFT_GROUP_INFO;
    }

    @Override
    public String toString() {
        return "RaftGroupInfo{" + "id=" + id + ", initialMembers=" + initialMembers + ", membersCommitIndex=" + membersCommitIndex
                + ", members=" + members() + ", status=" + status + '}';
    }
}
