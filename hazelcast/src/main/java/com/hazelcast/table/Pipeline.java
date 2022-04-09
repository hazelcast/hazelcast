package com.hazelcast.table;

public interface Pipeline {

    void noop(int partitionId);

    void execute();

    void await();
}
