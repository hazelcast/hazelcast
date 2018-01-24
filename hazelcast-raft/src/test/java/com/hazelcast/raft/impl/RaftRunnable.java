package com.hazelcast.raft.impl;

public interface RaftRunnable {

    Object run(Object service, long commitIndex);

}
