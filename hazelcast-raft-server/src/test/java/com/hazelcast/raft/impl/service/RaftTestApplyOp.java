package com.hazelcast.raft.impl.service;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
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
    public Object doRun(long commitIndex) {
        RaftDataService service = getService();
        return service.apply(commitIndex, val);
    }

    @Override
    public String getServiceName() {
        return RaftDataService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(val);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        val = in.readObject();
    }

    @Override
    public String toString() {
        return "RaftTestApplyOperation{" + "val=" + val + '}';
    }
}
