package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.dto.VoteResponse;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class VoteResponseOp extends AsyncRaftOp {

    private VoteResponse voteResponse;

    public VoteResponseOp() {
    }

    public VoteResponseOp(String name, VoteResponse voteResponse) {
        super(name);
        this.voteResponse = voteResponse;
    }

    @Override
    public void run() throws Exception {
        RaftService service = getService();
        service.handleVoteResponse(name, voteResponse);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(voteResponse);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        voteResponse = in.readObject();
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.VOTE_RESPONSE_OP;
    }
}
