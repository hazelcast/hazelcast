package com.hazelcast.raft.impl.service;

import com.hazelcast.raft.impl.testing.RaftRunnable;

public class QueryRaftRunnable implements RaftRunnable {

    @Override
    public Object run(Object service, long commitIndex) {
        return ((RaftDataService) service).get(commitIndex);
    }

}
