package com.hazelcast.raft.impl.service.proxy;

import com.hazelcast.raft.impl.RaftNode;

/**
 * A query operation that requires to access RaftNode state can implement this interface
 */
public interface RaftNodeAware {
    void setRaftNode(RaftNode raftNode);
}
