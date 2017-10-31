package com.hazelcast.raft.impl.service.proxy;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.service.CreateRaftGroupOperation;
import com.hazelcast.raft.impl.service.RaftService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CreateRaftGroupReplicatingOperation extends RaftReplicatingOperation {

    private String name;
    private int nodeCount;

    public CreateRaftGroupReplicatingOperation() {
    }

    public CreateRaftGroupReplicatingOperation(String name, int nodeCount) {
        this.name = name;
        this.nodeCount = nodeCount;
    }

    @Override
    public void run() throws Exception {
        RaftService service = getService();
        Collection<RaftEndpoint> allEndpoints = service.getAllEndpoints();
        List<RaftEndpoint> endpoints = new ArrayList<RaftEndpoint>(allEndpoints);
        Collections.shuffle(endpoints);
        endpoints = endpoints.subList(0, nodeCount);
        replicate(new CreateRaftGroupOperation(name, endpoints), RaftService.METADATA_RAFT);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeInt(nodeCount);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        nodeCount = in.readInt();
    }
}
