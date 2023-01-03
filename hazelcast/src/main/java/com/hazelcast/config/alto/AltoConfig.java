package com.hazelcast.config.alto;

import com.hazelcast.config.InvalidConfigurationException;

import java.util.Objects;

public class AltoConfig {
    private boolean enabled = false;
    private int eventloopCount = Runtime.getRuntime().availableProcessors();

    public boolean isEnabled() {
        return enabled;
    }

    public AltoConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public int getEventloopCount() {
        return eventloopCount;
    }

    public AltoConfig setEventloopCount(int eventloopCount) {
        if (eventloopCount < 1) {
            throw new InvalidConfigurationException("Buffer size should be a positive number");
        }

        this.eventloopCount = eventloopCount;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AltoConfig that = (AltoConfig) o;
        return enabled == that.enabled
                && eventloopCount == that.eventloopCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, eventloopCount);
    }

    @Override
    public String toString() {
        return "AltoConfig{"
                + "enabled=" + enabled
                + ", eventloopCount=" + eventloopCount
                + '}';
    }
}
