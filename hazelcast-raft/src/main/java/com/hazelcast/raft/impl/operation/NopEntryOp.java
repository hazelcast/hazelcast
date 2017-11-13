package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.operation.RaftOperation;

/**
 * No-op entry operation which is appended in the Raft log when a new leader is elected and
 * {@link com.hazelcast.raft.RaftConfig#appendNopEntryOnLeaderElection} is enabled.
 */
public class NopEntryOp extends RaftOperation implements IdentifiedDataSerializable {

    @Override
    protected Object doRun(long commitIndex) {
        return null;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.NOP_ENTRY_OP;
    }
}
