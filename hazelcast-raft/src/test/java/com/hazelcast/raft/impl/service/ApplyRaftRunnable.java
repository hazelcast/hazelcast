package com.hazelcast.raft.impl.service;

import com.hazelcast.raft.impl.testing.RaftRunnable;

public class ApplyRaftRunnable implements RaftRunnable {

    private Object val;

    public ApplyRaftRunnable(Object val) {
        this.val = val;
    }

    @Override
    public Object run(Object service, long commitIndex) {
        return ((RaftDataService) service).apply(commitIndex, val);
    }

    @Override
    public String toString() {
        return "ApplyRaftRunnable{" + "val=" + val + '}';
    }
}
