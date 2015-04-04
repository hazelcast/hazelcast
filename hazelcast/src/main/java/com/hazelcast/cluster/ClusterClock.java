package com.hazelcast.cluster;

public interface ClusterClock {

    /**
     * Returns the cluster-time in milliseconds.
     *
     * The cluster-time measures elapsed time since the cluster was created. Comparable to the {@link System#nanoTime()}.
     *
     * @return the cluster-time (elapsed milliseconds since the cluster was created).
     */
    long getClusterTime();

    long getClusterTimeDiff();
}
