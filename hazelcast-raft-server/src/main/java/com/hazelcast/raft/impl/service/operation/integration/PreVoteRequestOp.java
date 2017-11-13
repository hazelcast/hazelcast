package com.hazelcast.raft.impl.service.operation.integration;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class PreVoteRequestOp extends AsyncRaftOp {

    private PreVoteRequest voteRequest;

    public PreVoteRequestOp() {
    }

    public PreVoteRequestOp(RaftGroupId groupId, PreVoteRequest voteRequest) {
        super(groupId);
        this.voteRequest = voteRequest;
    }

    @Override
    public void run() throws Exception {
        RaftService service = getService();
        service.handlePreVoteRequest(groupId, voteRequest);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(voteRequest);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        voteRequest = in.readObject();
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.PRE_VOTE_REQUEST_OP;
    }
}
