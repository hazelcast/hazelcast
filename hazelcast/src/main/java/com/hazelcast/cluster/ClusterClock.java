package com.hazelcast.cluster;

public interface ClusterClock {

    /**
     * Returns the cluster-time in ms.
     *
     * The cluster-time can only be used to measure elapsed time. Comparable to the {@link System#nanoTime()}
     *
     * @return the cluster-time.
     */
    long getClusterTime();

    long getClusterTimeDiff();
}
