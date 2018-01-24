package com.hazelcast.raft.impl.service;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftEndpointImpl;
import com.hazelcast.raft.RaftGroupId;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class LeavingRaftEndpointContext implements IdentifiedDataSerializable {

    private RaftEndpointImpl endpoint;

    private Map<RaftGroupId, RaftGroupLeavingEndpointContext> groups;

    public LeavingRaftEndpointContext() {
    }

    public LeavingRaftEndpointContext(RaftEndpointImpl endpoint, Map<RaftGroupId, RaftGroupLeavingEndpointContext> groups) {
        this.endpoint = endpoint;
        this.groups = groups;
    }

    public RaftEndpointImpl getEndpoint() {
        return endpoint;
    }

    public Map<RaftGroupId, RaftGroupLeavingEndpointContext> getGroups() {
        return groups;
    }

    public LeavingRaftEndpointContext exclude(Collection<RaftGroupId> groupIds) {
        Map<RaftGroupId, RaftGroupLeavingEndpointContext> groups = new HashMap<RaftGroupId, RaftGroupLeavingEndpointContext>(this.groups);
        for (RaftGroupId leftGroupId : groupIds) {
            groups.remove(leftGroupId);
        }

        return new LeavingRaftEndpointContext(endpoint, groups);
    }

    public static class RaftGroupLeavingEndpointContext implements DataSerializable {

        private long membersCommitIndex;

        private Collection<RaftEndpointImpl> members;

        private RaftEndpointImpl substitute;

        public RaftGroupLeavingEndpointContext() {
        }

        public RaftGroupLeavingEndpointContext(long membersCommitIndex, Collection<RaftEndpointImpl> members, RaftEndpointImpl substitute) {
            this.membersCommitIndex = membersCommitIndex;
            this.members = members;
            this.substitute = substitute;
        }

        public long getMembersCommitIndex() {
            return membersCommitIndex;
        }

        public Collection<RaftEndpointImpl> getMembers() {
            return members;
        }

        public RaftEndpointImpl getSubstitute() {
            return substitute;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(membersCommitIndex);
            out.writeInt(members.size());
            for (RaftEndpointImpl member : members) {
                out.writeObject(member);
            }
            out.writeObject(substitute);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            membersCommitIndex = in.readLong();
            int len = in.readInt();
            members = new HashSet<RaftEndpointImpl>(len);
            for (int i = 0; i < len; i++) {
                RaftEndpointImpl member = in.readObject();
                members.add(member);
            }
            substitute = in.readObject();
        }

        @Override
        public String toString() {
            return "RaftGroupLeavingEndpointContext{" + "membersCommitIndex=" + membersCommitIndex + ", substitute=" + substitute + '}';
        }
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.LEAVING_RAFT_ENDPOINT_CTX;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(endpoint);
        out.writeInt(groups.size());
        for (Map.Entry<RaftGroupId, RaftGroupLeavingEndpointContext> entry : groups.entrySet()) {
            out.writeObject(entry.getKey());
            entry.getValue().writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        endpoint = in.readObject();
        int len = in.readInt();
        groups = new HashMap<RaftGroupId, RaftGroupLeavingEndpointContext>(len);
        for (int i = 0; i < len; i++) {
            RaftGroupId groupId = in.readObject();
            RaftGroupLeavingEndpointContext context = new RaftGroupLeavingEndpointContext();
            context.readData(in);
            groups.put(groupId, context);
        }
    }

    @Override
    public String toString() {
        return "LeavingRaftEndpointContext{" + "endpoint=" + endpoint + ", groups=" + groups + '}';
    }
}
