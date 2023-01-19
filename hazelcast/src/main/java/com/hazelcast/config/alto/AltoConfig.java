package com.hazelcast.config.alto;

import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
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
     * Gets the enabled flag which defines Alto is enabled or not.
     *
     * @return true if Alto is enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets the enabled flag which defines Alto is enabled or not.
     *
     * @param enabled a boolean to enable or disable alto
     * @return this Alto configuration
     */
    @Nonnull
    public AltoConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Gets the number of eventloops.
     *
     * @return the number of eventloop threads
     * @see Runtime#availableProcessors()
     * @see com.hazelcast.internal.tpc.Eventloop
     * @see AltoConfig#setEventloopCount(int)
     */
    public int getEventloopCount() {
        return eventloopCount;
    }

    /**
     * In Alto, everything is done in eventloops. This method sets the
     * number eventloops. By default, it's equal to the number of
     * available processors.
     *
     * @param eventloopCount the number of eventloop threads to set
     * @return this Alto configuration
     * @throws IllegalArgumentException if eventloopCount isn't positive
     * @see Runtime#availableProcessors()
     * @see com.hazelcast.internal.tpc.Eventloop
     */
    @Nonnull
    public AltoConfig setEventloopCount(int eventloopCount) {
        this.eventloopCount = checkPositive("eventloopCount", eventloopCount);
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
