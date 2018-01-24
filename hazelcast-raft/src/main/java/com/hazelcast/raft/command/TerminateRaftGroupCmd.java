package com.hazelcast.raft.command;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;

/**
 * A {@code RaftGroupCmd} to terminate an existing Raft group.
 */
public class TerminateRaftGroupCmd extends RaftGroupCmd implements IdentifiedDataSerializable {

    public TerminateRaftGroupCmd() {
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.TERMINATE_RAFT_GROUP_COMMAND;
    }

    @Override
    public void writeData(ObjectDataOutput out) {
    }

    @Override
    public void readData(ObjectDataInput in) {
    }

    @Override
    public String toString() {
        return "TerminateRaftGroupCmd{}";
    }
}
