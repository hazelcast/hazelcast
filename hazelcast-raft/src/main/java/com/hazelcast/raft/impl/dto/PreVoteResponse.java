package com.hazelcast.raft.impl.dto;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;

/**
 * Struct for response to PreVoteRequest RPC.
 * <p>
 * See <i>Four modifications for the Raft consensus algorithm</i> by Henrik Ingo.
 *
 * @see PreVoteRequest
 * @see VoteResponse
 */
public class PreVoteResponse implements IdentifiedDataSerializable {

    private RaftEndpoint voter;
    private int term;
    private boolean granted;

    public PreVoteResponse() {
    }

    public PreVoteResponse(RaftEndpoint voter, int term, boolean granted) {
        this.voter = voter;
        this.term = term;
        this.granted = granted;
    }

    public RaftEndpoint voter() {
        return voter;
    }

    public int term() {
        return term;
    }

    public boolean granted() {
        return granted;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.PRE_VOTE_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(term);
        out.writeBoolean(granted);
        out.writeObject(voter);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        term = in.readInt();
        granted = in.readBoolean();
        voter = in.readObject();
    }

    @Override
    public String toString() {
        return "PreVoteResponse{" + "voter=" + voter + ", term=" + term + ", granted=" + granted + '}';
    }

}
