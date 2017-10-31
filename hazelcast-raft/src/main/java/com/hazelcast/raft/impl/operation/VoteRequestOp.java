package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.dto.VoteRequest;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class VoteRequestOp extends AsyncRaftOp {

    private VoteRequest voteRequest;

    public VoteRequestOp() {
    }

    public VoteRequestOp(String name, VoteRequest voteRequest) {
        super(name);
        this.voteRequest = voteRequest;
    }

    @Override
    public void run() throws Exception {
        RaftService service = getService();
        service.handleVoteRequest(name, voteRequest);
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
        return RaftDataSerializerHook.VOTE_REQUEST_OP;
    }
}
