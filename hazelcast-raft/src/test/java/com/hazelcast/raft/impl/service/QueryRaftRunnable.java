package com.hazelcast.raft.impl.service;

import com.hazelcast.raft.impl.RaftRunnable;

public class QueryRaftRunnable implements RaftRunnable {

    @Override
    public Object run(Object service, long commitIndex) {
        return ((RaftDataService) service).get(commitIndex);
    }

}
