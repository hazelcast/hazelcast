package com.hazelcast.raft.impl.service;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;

import java.io.IOException;

public class RaftTestApplyOp extends RaftOp {

    private Object val;

    public RaftTestApplyOp() {
    }

    public RaftTestApplyOp(Object val) {
        this.val = val;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftDataService service = getService();
        return service.apply(commitIndex, val);
    }

    @Override
    public String getServiceName() {
        return RaftDataService.SERVICE_NAME;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(val);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        val = in.readObject();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", val=").append(val);
    }
}
