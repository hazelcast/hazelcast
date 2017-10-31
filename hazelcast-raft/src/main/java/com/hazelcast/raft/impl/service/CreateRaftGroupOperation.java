package com.hazelcast.raft.impl.service;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CreateRaftGroupOperation extends RaftOperation {

    private String name;
    private Collection<RaftEndpoint> endpoints;

    public CreateRaftGroupOperation() {
    }

    public CreateRaftGroupOperation(String name, Collection<RaftEndpoint> endpoints) {
        this.name = name;
        this.endpoints = endpoints;
    }

    @Override
    public Object doRun(int commitIndex) {
        RaftService service = getService();
        service.addRaftNode(name, endpoints);
        return true;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeInt(endpoints.size());
        for (RaftEndpoint endpoint : endpoints) {
            out.writeObject(endpoint);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        int len = in.readInt();
        endpoints = new ArrayList<RaftEndpoint>(len);
        for (int i = 0; i < len; i++) {
            RaftEndpoint endpoint = in.readObject();
            endpoints.add(endpoint);
        }
    }
}
