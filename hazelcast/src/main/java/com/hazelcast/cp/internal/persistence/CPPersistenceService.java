package com.hazelcast.cp.internal.persistence;

import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.raft.impl.persistence.LogFileStructure;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * TODO: Javadoc Pending...
 *
 */
public interface CPPersistenceService {

    boolean isEnabled();

    /**
     * TODO
     */
    CPMemberMetadataStore getCPMemberMetadataStore();

    /**
     * TODO
     */
    RaftStateStore createRaftStateStore(@Nonnull RaftGroupId groupId, @Nullable LogFileStructure logFileStructure);

    /**
     * TODO
     * Remove all persisted data...
     */
    void reset();
}
