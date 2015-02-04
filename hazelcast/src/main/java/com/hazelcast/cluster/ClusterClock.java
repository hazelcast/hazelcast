package com.hazelcast.cluster;

public interface ClusterClock {

    /**
     * Returns the cluster-time.
     * <p/>
     * TODO: We need to document what cluster time really means and what is can be used for.
     *
     * @return the cluster-time.
     */
    long getClusterTime();

    long getClusterTimeDiff();
}
