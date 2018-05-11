package com.hazelcast.raft.exception;

import com.hazelcast.config.raft.RaftAlgorithmConfig;
import com.hazelcast.raft.RaftMember;
import com.hazelcast.spi.exception.RetryableException;

/**
 * A {@code RaftException} which is thrown when an entry cannot be replicated.
 * An append request can be rejected when,
 * <ul>
 * <li>a member leaves the Raft group</li>
 * <li>Raft group itself is terminated</li>
 * <li>uncommitted entry count reaches to max (see {@link RaftAlgorithmConfig#uncommittedEntryCountToRejectNewAppends})</li>
 * <li>a membership change is requested before an entry is committed on a term</li>
 * </ul>
 */
public class CannotReplicateException extends RaftException implements RetryableException {

    public CannotReplicateException(RaftMember leader) {
        super("Cannot replicate new operations for now", leader);
    }
}
