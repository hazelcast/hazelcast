package com.hazelcast.internal.partition.impl;

import com.hazelcast.nio.Address;

public interface MigrationRunnable extends Runnable {

    void invalidate(Address address);

    boolean isValid();
    
    boolean isPauseable();

}
