package com.hazelcast.monitor.impl;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.monitor.LocalScheduledExecutorStats;

import java.util.concurrent.atomic.AtomicLong;

public class LocalScheduledExecutorStatsImpl implements LocalScheduledExecutorStats {

    private final AtomicLong scheduledSingleRun = new AtomicLong();
    private final AtomicLong scheduledFixedRate = new AtomicLong();

    public void incScheduledSingleRun() {
        scheduledSingleRun.incrementAndGet();
    }

    public void incScheduledFixedRate() {
        scheduledFixedRate.incrementAndGet();
    }

    @Override
    public long getCreationTime() {
        return 0;
    }

    @Override
    public JsonObject toJson() {
        return null;
    }

    @Override
    public void fromJson(JsonObject json) {

    }

}
