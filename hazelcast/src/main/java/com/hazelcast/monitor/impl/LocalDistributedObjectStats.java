package com.hazelcast.monitor.impl;

import com.hazelcast.monitor.LocalInstanceStats;

/**
 * Base class for {@link LocalInstanceStats} on distributed objects. 
 */
public abstract class LocalDistributedObjectStats implements LocalInstanceStats {

    private final boolean statisticsEnabled;

    LocalDistributedObjectStats(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
    }

    public final boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

}
