package com.hazelcast.config.tpc;

import com.hazelcast.config.InvalidConfigurationException;

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
        if (eventloopCount < Bounds.MIN_EVENTLOOP_COUNT || eventloopCount > Bounds.MAX_EVENTLOOP_COUNT) {
            throw new InvalidConfigurationException("Buffer size should be between "
                    + Bounds.MIN_EVENTLOOP_COUNT + " and " + Bounds.MAX_EVENTLOOP_COUNT);
        }

        this.eventloopCount = eventloopCount;
        return this;
    }

    private static class Bounds {
        private static final int MIN_EVENTLOOP_COUNT = 1;
        private static final int MAX_EVENTLOOP_COUNT = 256;
    }
}
