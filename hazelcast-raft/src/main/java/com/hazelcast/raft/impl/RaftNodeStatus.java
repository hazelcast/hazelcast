package com.hazelcast.raft.impl;

/**
 * Represents status of a {@link RaftNode} during its lifecycle.
 * <p>
 * Initially, a Raft node starts as {@link #ACTIVE}.
 */
public enum RaftNodeStatus {
    /**
     * Initial state of a node. When {@code ACTIVE} node operates normally.
     */
    ACTIVE,

    /**
     * During membership changes, node statuses become {@code CHANGING_MEMBERSHIP}
     * and they apply requested change once the entry is appended to the log.
     * Once log is committed, if the related node is the being removed from group,
     * status becomes {@link #STEPPED_DOWN}, otherwise {@link #ACTIVE}.
     */
    CHANGING_MEMBERSHIP,

    /**
     * When a node is removed from the cluster after a membership change is committed,
     * its status becomes {@code STEPPED_DOWN}.
     */
    STEPPED_DOWN,

    /**
     * When a Raft group is being terminated, all nodes' statuses in that group become {@code TERMINATING}.
     * Once termination process is completed, then statuses become {@link #TERMINATED}.
     */
    TERMINATING,

    /**
     * When a Raft group is terminated completely, all nodes' statuses in that group become {@code TERMINATED}.
     */
    TERMINATED

}
