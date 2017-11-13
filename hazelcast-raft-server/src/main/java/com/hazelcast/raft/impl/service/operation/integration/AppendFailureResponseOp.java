package com.hazelcast.raft.impl.service.operation.integration;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftService;

import java.io.IOException;

public class AppendFailureResponseOp extends AsyncRaftOp {

    private AppendFailureResponse appendResponse;

    public AppendFailureResponseOp() {
    }

    public AppendFailureResponseOp(RaftGroupId groupId, AppendFailureResponse appendResponse) {
        super(groupId);
        this.appendResponse = appendResponse;
    }

    @Override
    public void run() throws Exception {
        RaftService service = getService();
        service.handleAppendResponse(groupId, appendResponse);
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
        return RaftServiceDataSerializerHook.APPEND_FAILURE_RESPONSE_OP;
    }

}
