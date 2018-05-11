package com.hazelcast.raft.impl.dto;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.RaftMember;

import java.io.IOException;

/**
 * Struct for response to VoteRequest RPC.
 * <p>
 * See <i>5.2 Leader election</i> section of <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @see VoteRequest
 */
public class VoteResponse implements IdentifiedDataSerializable {

    private RaftMember voter;
    private int term;
    private boolean granted;

    public VoteResponse() {
    }

    public VoteResponse(RaftMember voter, int term, boolean granted) {
        this.voter = voter;
        this.term = term;
        this.granted = granted;
    }

    public RaftMember voter() {
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
        return RaftDataSerializerHook.VOTE_RESPONSE;
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
        return "VoteResponse{" + "voter=" + voter + ", term=" + term + ", granted=" + granted + '}';
    }

}
