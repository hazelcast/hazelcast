package com.hazelcast.core;

public interface ICardinalityEstimator {

    void addHash(long hash);

    long estimate();

}
