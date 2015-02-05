package com.hazelcast.cluster.impl;

import com.hazelcast.cluster.ClusterClock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.Clock;

public class ClusterClockImpl implements ClusterClock {

    private final ILogger logger;
    private volatile long clusterTimeDiff = Long.MAX_VALUE;

    public ClusterClockImpl(ILogger logger) {
        this.logger = logger;
    }

    @Override
    public long getClusterTime() {
        return Clock.currentTimeMillis() + ((clusterTimeDiff == Long.MAX_VALUE) ? 0 : clusterTimeDiff);
    }

    public void setMasterTime(long masterTime) {
        long diff = masterTime - Clock.currentTimeMillis();
        if (logger.isFinestEnabled()) {
            logger.finest("Setting cluster time diff to " + diff + "ms.");
        }
        this.clusterTimeDiff = diff;
    }

    @Override
    public long getClusterTimeDiff() {
        return (clusterTimeDiff == Long.MAX_VALUE) ? 0 : clusterTimeDiff;
    }
}
