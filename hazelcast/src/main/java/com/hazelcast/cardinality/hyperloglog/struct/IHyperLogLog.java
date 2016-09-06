package com.hazelcast.cardinality.hyperloglog.struct;

public interface IHyperLogLog {

    long estimate();

    boolean aggregate(long hash);
    boolean aggregate(long[] hashes);

}
