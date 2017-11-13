package com.hazelcast.raft.impl.service;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 *
 */
public final class MetadataSnapshot implements IdentifiedDataSerializable {

    private final Collection<RaftEndpoint> endpoints = new ArrayList<RaftEndpoint>();
    private final Collection<RaftEndpoint> removedEndpoints = new ArrayList<RaftEndpoint>();
    private final Collection<RaftGroupInfo> raftGroups = new ArrayList<RaftGroupInfo>();
    private LeavingRaftEndpointContext leavingRaftEndpointContext;

    public void addRaftGroup(RaftGroupInfo groupInfo) {
        raftGroups.add(groupInfo);
    }

    public void addEndpoint(RaftEndpoint endpoint) {
        endpoints.add(endpoint);
    }

    public void addRemovedEndpoint(RaftEndpoint endpoint) {
        removedEndpoints.add(endpoint);
    }

    public Collection<RaftEndpoint> getEndpoints() {
        return endpoints;
    }

    public Collection<RaftEndpoint> getRemovedEndpoints() {
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
        for (RaftEndpoint endpoint : endpoints) {
            out.writeObject(endpoint);
        }
        out.writeInt(raftGroups.size());
        for (RaftGroupInfo group : raftGroups) {
            out.writeObject(group);
        }
        out.writeInt(removedEndpoints.size());
        for (RaftEndpoint endpoint : removedEndpoints) {
            out.writeObject(endpoint);
        }
        out.writeObject(leavingRaftEndpointContext);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        for (int i = 0; i < len; i++) {
            RaftEndpoint endpoint = in.readObject();
            endpoints.add(endpoint);
        }

        len = in.readInt();
        for (int i = 0; i < len; i++) {
            RaftGroupInfo groupInfo = in.readObject();
            raftGroups.add(groupInfo);
        }

        len = in.readInt();
        for (int i = 0; i < len; i++) {
            RaftEndpoint endpoint = in.readObject();
            removedEndpoints.add(endpoint);
        }
        leavingRaftEndpointContext = in.readObject();
    }
}
