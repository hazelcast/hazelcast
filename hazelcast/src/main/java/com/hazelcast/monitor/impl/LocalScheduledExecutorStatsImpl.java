package com.hazelcast.monitor.impl;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.monitor.LocalScheduledExecutorStats;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskDescriptor;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class LocalScheduledExecutorStatsImpl implements LocalScheduledExecutorStats {

    @Probe
    private final AtomicLong scheduledSingleRun = new AtomicLong();
    @Probe
    private final AtomicLong scheduledFixedRate = new AtomicLong();
    public volatile ConcurrentMap<String, ScheduledTaskDescriptor> tasks;

    @Probe
    public int tasks() {
        ConcurrentMap<String, ScheduledTaskDescriptor> tasks = this.tasks;
        return tasks == null ? 0 : tasks.size();
    }

    public long scheduledSingleRun() {
        return scheduledSingleRun.get();
    }

    public long scheduledFixedRate() {
        return scheduledFixedRate.get();
    }

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
