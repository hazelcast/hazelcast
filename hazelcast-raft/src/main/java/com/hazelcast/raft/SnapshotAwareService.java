package com.hazelcast.raft;

/**
 * Service interface required to be implemented by services participating in Raft state.
 * Each Raft service must be able to generate a snapshot of its committed data and restore it back.
 *
 * @param <T> type of snapshot object
 */
public interface SnapshotAwareService<T> {

    /**
     * Creates a snapshot for specified {@link RaftGroupId}.
     *
     * @param groupId {@link RaftGroupId} which is snapshot requested for
     * @param commitIndex commitIndex of the Raft state when the snapshot is requested
     * @return snapshot for specified {@link RaftGroupId}.
     */
    T takeSnapshot(RaftGroupId groupId, long commitIndex);

    /**
     * Restores the snapshot for specified {@link RaftGroupId}.
     *
     * @param groupId {@link RaftGroupId} of the snapshot to be restored
     * @param commitIndex commitIndex of the Raft state when snapshot is created
     * @param snapshot snapshot for specified {@link RaftGroupId}
     */
    void restoreSnapshot(RaftGroupId groupId, long commitIndex, T snapshot);

}
