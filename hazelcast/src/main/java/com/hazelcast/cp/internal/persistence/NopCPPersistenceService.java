package com.hazelcast.cp.internal.persistence;

import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.raft.impl.persistence.LogFileStructure;
import com.hazelcast.cp.internal.raft.impl.persistence.NopRaftStateStore;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class NopCPPersistenceService implements CPPersistenceService {

    public static final CPPersistenceService INSTANCE = new NopCPPersistenceService();

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public CPMemberMetadataStore getCPMemberMetadataStore() {
        return NopCPMemberMetadataStore.INSTANCE;
    }

    @Override
    public RaftStateStore createRaftStateStore(@Nonnull RaftGroupId groupId, @Nullable LogFileStructure logFileStructure) {
        return NopRaftStateStore.INSTANCE;
    }

    @Override
    public void reset() {
    }
}
