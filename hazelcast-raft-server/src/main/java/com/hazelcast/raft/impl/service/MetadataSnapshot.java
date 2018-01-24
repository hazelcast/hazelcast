package com.hazelcast.raft.impl.service;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftEndpointImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 *
 */
public final class MetadataSnapshot implements IdentifiedDataSerializable {

    private final Collection<RaftEndpointImpl> endpoints = new ArrayList<RaftEndpointImpl>();
    private final Collection<RaftEndpointImpl> removedEndpoints = new ArrayList<RaftEndpointImpl>();
    private final Collection<RaftGroupInfo> raftGroups = new ArrayList<RaftGroupInfo>();
    private LeavingRaftEndpointContext leavingRaftEndpointContext;

    public void addRaftGroup(RaftGroupInfo groupInfo) {
        raftGroups.add(groupInfo);
    }

    public void addEndpoint(RaftEndpointImpl endpoint) {
        endpoints.add(endpoint);
    }

    public void addRemovedEndpoint(RaftEndpointImpl endpoint) {
        removedEndpoints.add(endpoint);
    }

    public Collection<RaftEndpointImpl> getEndpoints() {
        return endpoints;
    }

    public Collection<RaftEndpointImpl> getRemovedEndpoints() {
        return removedEndpoints;
    }

    public Collection<RaftGroupInfo> getRaftGroups() {
        return raftGroups;
    }

    public LeavingRaftEndpointContext getLeavingRaftEndpointContext() {
        return leavingRaftEndpointContext;
    }

    public void setLeavingRaftEndpointContext(LeavingRaftEndpointContext leavingRaftEndpointContext) {
        this.leavingRaftEndpointContext = leavingRaftEndpointContext;
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
        out.writeInt(endpoints.size());
        for (RaftEndpointImpl endpoint : endpoints) {
            out.writeObject(endpoint);
        }
        out.writeInt(raftGroups.size());
        for (RaftGroupInfo group : raftGroups) {
            out.writeObject(group);
        }
        out.writeInt(removedEndpoints.size());
        for (RaftEndpointImpl endpoint : removedEndpoints) {
            out.writeObject(endpoint);
        }
        out.writeObject(leavingRaftEndpointContext);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        for (int i = 0; i < len; i++) {
            RaftEndpointImpl endpoint = in.readObject();
            endpoints.add(endpoint);
        }

        len = in.readInt();
        for (int i = 0; i < len; i++) {
            RaftGroupInfo groupInfo = in.readObject();
            raftGroups.add(groupInfo);
        }

        len = in.readInt();
        for (int i = 0; i < len; i++) {
            RaftEndpointImpl endpoint = in.readObject();
            removedEndpoints.add(endpoint);
        }
        leavingRaftEndpointContext = in.readObject();
    }
}
