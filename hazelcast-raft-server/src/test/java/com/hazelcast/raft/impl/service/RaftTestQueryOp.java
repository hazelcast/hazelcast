package com.hazelcast.raft.impl.service;

import com.hazelcast.raft.impl.RaftOp;

public class RaftTestQueryOp extends RaftOp {

    @Override
    public Object doRun(long commitIndex) {
        RaftDataService service = getService();
        return service.get(commitIndex);
    }

    @Override
    public String getServiceName() {
        return RaftDataService.SERVICE_NAME;
    }

}
