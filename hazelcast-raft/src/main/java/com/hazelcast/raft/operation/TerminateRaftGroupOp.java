package com.hazelcast.raft.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;

import java.io.IOException;

/**
 * A {@code RaftCommandOperation} to terminate an existing Raft group.
 */
public class TerminateRaftGroupOp extends RaftCommandOperation implements IdentifiedDataSerializable {

    public TerminateRaftGroupOp() {
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.TERMINATE_RAFT_GROUP_OP;
    }

    @Override
    public String toString() {
        return "TerminateRaftGroupOp{}";
    }
}
