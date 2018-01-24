package com.hazelcast.raft.impl.log;

import com.hazelcast.config.raft.RaftConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;

/**
 * No-op entry operation which is appended in the Raft log when a new leader is elected and
 * {@link RaftConfig#appendNopEntryOnLeaderElection} is enabled.
 */
public class NopEntry implements IdentifiedDataSerializable {
    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.NOP_ENTRY;
    }

    @Override
    public void writeData(ObjectDataOutput out) {
    }

    @Override
    public void readData(ObjectDataInput in) {
    }
}
