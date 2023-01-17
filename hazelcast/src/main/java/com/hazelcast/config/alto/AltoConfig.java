package com.hazelcast.config.alto;

import com.hazelcast.spi.annotation.Beta;

import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Hazelcast Alto is the next generation of Hazelcast built with thread
 * per core architecture. It's still being developed and everything is
 * subject to change. Alto is disabled by default.
 *
 * @since 5.3
 */
@Beta
public class AltoConfig {
    private boolean enabled = false;
    private int eventloopCount = Runtime.getRuntime().availableProcessors();

    /**
     * @return true if alto is enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * @param enabled a boolean to enable or disable alto
     * @return this alto configuration
     */
    public AltoConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * In Alto, everything is done in threads called Eventloop. This method
     * gets the number of such threads. By default, it's equal to the
     * number of available processors.
     *
     * @return the number of eventloop threads
     * @see Runtime#availableProcessors()
     * @see com.hazelcast.internal.tpc.Eventloop
     */
    public int getEventloopCount() {
        return eventloopCount;
    }

    /**
     * In Alto, everything is done in threads called Eventloop. This method
     * gets the number of such threads. By default, it's equal to the
     * number of available processors.
     *
     * @param eventloopCount the number of eventloop threads to set
     * @return this alto configuration
     * @see Runtime#availableProcessors()
     * @see com.hazelcast.internal.tpc.Eventloop
     */
    public AltoConfig setEventloopCount(int eventloopCount) {
        checkPositive("eventloopCount", eventloopCount);
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
