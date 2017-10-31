package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;

import java.io.IOException;

public class AppendFailureResponseOp extends AsyncRaftOp {

    private AppendFailureResponse appendResponse;

    public AppendFailureResponseOp() {
    }

    public AppendFailureResponseOp(String name, AppendFailureResponse appendResponse) {
        super(name);
        this.appendResponse = appendResponse;
    }

    @Override
    public void run() throws Exception {
        RaftService service = getService();
        service.handleAppendResponse(name, appendResponse);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(appendResponse);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        appendResponse = in.readObject();
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.APPEND_FAILURE_RESPONSE_OP;
    }

}
