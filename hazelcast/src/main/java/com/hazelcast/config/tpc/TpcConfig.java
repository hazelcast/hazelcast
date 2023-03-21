/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.config.tpc;

import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Hazelcast TPC is the next generation of Hazelcast built with thread
 * per core architecture. It's still being developed and everything is
 * subject to change. TPC is disabled by default.
 *
 * @since 5.3
 */
@Beta
public class TpcConfig {
    private boolean enabled;
    private int eventloopCount = Runtime.getRuntime().availableProcessors();

    /**
     * Gets the enabled flag which defines TPC is enabled or not.
     *
     * @return true if TPC is enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets the enabled flag which defines TPC is enabled or not. Can't
     * return null.
     *
     * @param enabled a boolean to enable or disable TPC
     * @return this
     */
    @Nonnull
    public TpcConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Gets the number of eventloops.
     *
     * @return the number of eventloops
     * @see Runtime#availableProcessors()
     * @see TpcConfig#setEventloopCount(int)
     */
    public int getEventloopCount() {
        return eventloopCount;
    }

    /**
     * In TPC, everything is done in eventloops. This method sets the
     * number eventloops. By default, it's equal to the number of
     * available processors. Can't return null.
     *
     * @param eventloopCount the number of eventloops
     * @return this
     * @throws IllegalArgumentException if eventloopCount isn't positive
     * @see Runtime#availableProcessors()
     */
    @Nonnull
    public TpcConfig setEventloopCount(int eventloopCount) {
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
        TpcConfig that = (TpcConfig) o;
        return enabled == that.enabled
                && eventloopCount == that.eventloopCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, eventloopCount);
    }

    @Override
    public String toString() {
        return "TpcConfig{"
                + "enabled=" + enabled
                + ", eventloopCount=" + eventloopCount
                + '}';
    }
}
