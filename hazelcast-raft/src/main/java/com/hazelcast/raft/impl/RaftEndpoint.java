package com.hazelcast.raft.impl;

/**
 * {@code RaftEndpoint} represents a member in Raft group.
 * Each endpoint must have a unique id in the group.
 */
public interface RaftEndpoint {

    String getUid();

}
