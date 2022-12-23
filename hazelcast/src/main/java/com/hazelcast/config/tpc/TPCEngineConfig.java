package com.hazelcast.config.tpc;

public class TPCEngineConfig {
    private static final boolean DEFAULT_ENABLED = false;
    private static final int DEFAULT_EVENTLOOP_COUNT = Runtime.getRuntime().availableProcessors();

    private boolean enabled = DEFAULT_ENABLED;
    private int eventloopCount = DEFAULT_EVENTLOOP_COUNT;

    public boolean isEnabled() {
        return enabled;
    }

    public TPCEngineConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public int getEventloopCount() {
        return eventloopCount;
    }

    public TPCEngineConfig setEventloopCount(int eventloopCount) {
        this.eventloopCount = eventloopCount;
        return this;
    }
}
