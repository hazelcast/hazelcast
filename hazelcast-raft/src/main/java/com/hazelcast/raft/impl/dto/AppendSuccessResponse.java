package com.hazelcast.raft.impl.dto;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;

/**
 * Struct for successful response to AppendEntries RPC.
 * <p>
 * See <i>5.3 Log replication</i> section of <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @see AppendRequest
 * @see AppendFailureResponse
 */
public class AppendSuccessResponse implements IdentifiedDataSerializable {

    private RaftEndpoint follower;
    private int term;
    private long lastLogIndex;

    public AppendSuccessResponse() {
    }

    public AppendSuccessResponse(RaftEndpoint follower, int term, long lastLogIndex) {
        this.follower = follower;
        this.term = term;
        this.lastLogIndex = lastLogIndex;
    }

    public RaftEndpoint follower() {
        return follower;
    }

    public int term() {
        return term;
    }

    public long lastLogIndex() {
        return lastLogIndex;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.APPEND_SUCCESS_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(term);
        out.writeObject(follower);
        out.writeLong(lastLogIndex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        term = in.readInt();
        follower = in.readObject();
        lastLogIndex = in.readLong();
    }

    @Override
    public String toString() {
        return "AppendSuccessResponse{" + "follower=" + follower + ", term=" + term  + ", lastLogIndex="
                + lastLogIndex + '}';
    }

}
