package com.hazelcast.raft.impl.log;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.RaftMember;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

/**
 * Represents a snapshot in the {@link RaftLog}.
 * <p>
 * Snapshot entry is sent to followers via {@link com.hazelcast.raft.impl.dto.InstallSnapshot} RPC.
 */
public class SnapshotEntry extends LogEntry implements IdentifiedDataSerializable {
    private long groupMembersLogIndex;
    private Collection<RaftMember> groupMembers;

    public SnapshotEntry() {
    }

    public SnapshotEntry(int term, long index, Object operation,
                         long groupMembersLogIndex, Collection<RaftMember> groupMembers) {
        super(term, index, operation);
        this.groupMembersLogIndex = groupMembersLogIndex;
        this.groupMembers = groupMembers;
    }

    public long groupMembersLogIndex() {
        return groupMembersLogIndex;
    }

    public Collection<RaftMember> groupMembers() {
        return groupMembers;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeLong(groupMembersLogIndex);
        out.writeInt(groupMembers.size());
        for (RaftMember endpoint : groupMembers) {
            out.writeObject(endpoint);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        groupMembersLogIndex = in.readLong();
        int count = in.readInt();
        groupMembers = new HashSet<RaftMember>(count);
        for (int i = 0; i < count; i++) {
            RaftMember endpoint = in.readObject();
            groupMembers.add(endpoint);
        }
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.SNAPSHOT_ENTRY;
    }

    @Override
    public String toString() {
        return "SnapshotEntry{" + "term=" + term() + ", index=" + index() + ", operation=" + operation()
                + ", groupMembersLogIndex=" + groupMembersLogIndex + ", groupMembers=" + groupMembers + '}';
    }
}
