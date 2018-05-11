package com.hazelcast.raft.impl.service;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;

import java.io.IOException;

public class RaftTestQueryOp extends RaftOp {

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftDataService service = getService();
        return service.get(commitIndex);
    }

    @Override
    public String getServiceName() {
        return RaftDataService.SERVICE_NAME;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }
}
