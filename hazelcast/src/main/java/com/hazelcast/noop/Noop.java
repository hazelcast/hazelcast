package com.hazelcast.noop;

import com.hazelcast.core.TpcProxy;

public interface Noop extends TpcProxy {

    void noop(int partitionId);

    void concurrentNoop(int concurrency, int partitionId);
}
