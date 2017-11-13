package com.hazelcast.raft.impl.service;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus.ACTIVE;
import static com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus.DESTROYED;
import static com.hazelcast.raft.impl.service.RaftGroupInfo.RaftGroupStatus.DESTROYING;
import static com.hazelcast.util.Preconditions.checkState;

/**
 * TODO: Javadoc Pending...
 *
 */
public final class RaftGroupInfo implements IdentifiedDataSerializable {

    public enum RaftGroupStatus {
        ACTIVE, DESTROYING, DESTROYED
    }

    private RaftGroupId id;
    private long membersCommitIndex;
    // endpoint -> TRUE: initial-member | FALSE: substitute-member
    private Map<RaftEndpoint, Boolean> members;
    private String serviceName;

    // read outside of Raft
    private volatile RaftGroupStatus status;

    private transient RaftEndpoint[] membersArray;

    public RaftGroupInfo() {
    }

    public RaftGroupInfo(RaftGroupId id, Collection<RaftEndpoint> endpoints, String serviceName) {
        this.id = id;
        this.serviceName = serviceName;
        this.status = ACTIVE;
        LinkedHashMap<RaftEndpoint, Boolean> map = new LinkedHashMap<RaftEndpoint, Boolean>(endpoints.size());
        for (RaftEndpoint endpoint : endpoints) {
            map.put(endpoint, Boolean.TRUE);
        }
        this.members = Collections.unmodifiableMap(map);
        this.membersArray = endpoints.toArray(new RaftEndpoint[0]);
    }

    public RaftGroupId id() {
        return id;
    }

    public String name() {
        return id.name();
    }

    public long commitIndex() {
        return id.commitIndex();
    }

    public Collection<RaftEndpoint> members() {
        return members.keySet();
    }

    public boolean containsMember(RaftEndpoint endpoint) {
        return members.containsKey(endpoint);
    }

    public int memberCount() {
        return members.size();
    }

    public boolean isInitialMember(RaftEndpoint endpoint) {
        assert members.containsKey(endpoint);
        return members.get(endpoint);
    }

    public String serviceName() {
        return serviceName;
    }

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

        if (status == DESTROYED) {
            return false;
        }

        status = DESTROYED;
        return true;
    }

    public long getMembersCommitIndex() {
        return membersCommitIndex;
    }

    public boolean substitute(RaftEndpoint leaving, RaftEndpoint joining,
            long expectedMembersCommitIndex, long newMembersCommitIndex) {
        if (membersCommitIndex != expectedMembersCommitIndex) {
            return false;
        }

        Map<RaftEndpoint, Boolean> map = new LinkedHashMap<RaftEndpoint, Boolean>(members);
        Object removed = map.remove(leaving);
        assert removed != null : leaving + " is not member of " + toString();
        if (joining != null) {
            Object added = map.put(joining, Boolean.FALSE);
            assert added == null : joining + " is already member of " + toString();
        }

        members = Collections.unmodifiableMap(map);
        membersCommitIndex = newMembersCommitIndex;
        membersArray = members.keySet().toArray(new RaftEndpoint[0]);
        return true;
    }

    public RaftEndpoint[] membersArray() {
        return membersArray;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(id);
        out.writeLong(membersCommitIndex);
        out.writeInt(members.size());
        for (Map.Entry<RaftEndpoint, Boolean> entry : members.entrySet()) {
            out.writeObject(entry.getKey());
            out.writeBoolean(entry.getValue());
        }
        out.writeUTF(serviceName);
        out.writeUTF(status.toString());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readObject();
        membersCommitIndex = in.readLong();
        int len = in.readInt();
        members = new LinkedHashMap<RaftEndpoint, Boolean>(len);
        for (int i = 0; i < len; i++) {
            RaftEndpoint endpoint = in.readObject();
            members.put(endpoint, in.readBoolean());
        }
        members = Collections.unmodifiableMap(members);
        serviceName = in.readUTF();
        status = RaftGroupStatus.valueOf(in.readUTF());
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.GROUP_INFO;
    }

    @Override
    public String toString() {
        return "RaftGroupInfo{" + "id=" + id + ", membersCommitIndex=" + membersCommitIndex + ", members=" + members()
                + ", serviceName='" + serviceName + '\'' + ", status=" + status + '}';
    }
}
