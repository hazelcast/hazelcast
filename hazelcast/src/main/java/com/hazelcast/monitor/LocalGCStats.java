package com.hazelcast.monitor;

public interface LocalGCStats extends LocalInstanceStats {

    long getMajorCollectionCount();

    long getMajorCollectionTime();

    long getMinorCollectionCount();

    long getMinorCollectionTime();

    long getUnknownCollectionCount();

    long getUnknownCollectionTime();

}
