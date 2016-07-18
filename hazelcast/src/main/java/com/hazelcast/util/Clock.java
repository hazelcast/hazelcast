/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.nio.ClassLoaderUtil;

/**
 * Abstracts the system clock to simulate different clocks without changing the actual system time.
 *
 * Can be used to simulate different time zones or to control timing related behavior in Hazelcast.
 *
 * The time offset can be configured with the property {@value ClockProperties#HAZELCAST_CLOCK_OFFSET}.
 * The clock implementation can be configured with the property {@value ClockProperties#HAZELCAST_CLOCK_IMPL}.
 *
 * <b>WARNING:</b> This class is a singleton.
 * Once the class has been initialized, the clock implementation or offset cannot be changed.
 * To use this class properly in unit or integration tests, please have a look at {@code ClockTest}.
 */
public final class Clock {

    private static final ClockImpl CLOCK;

    private Clock() {
    }

    public static long currentTimeMillis() {
        return CLOCK.currentTimeMillis();
    }

    static {
        CLOCK = initClock();
    }

    private static ClockImpl initClock() {
        String clockImplClassName = System.getProperty(ClockProperties.HAZELCAST_CLOCK_IMPL);
        if (clockImplClassName != null) {
            try {
                return ClassLoaderUtil.newInstance(null, clockImplClassName);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }

        String clockOffset = System.getProperty(ClockProperties.HAZELCAST_CLOCK_OFFSET);
        long offset = 0L;
        if (clockOffset != null) {
            try {
                offset = Long.parseLong(clockOffset);
            } catch (NumberFormatException e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
        if (offset != 0L) {
            return new SystemOffsetClock(offset);
        }

        return new SystemClock();
    }

    /**
     * Extend this class if you want to provide your own clock implementation.
     */
    public abstract static class ClockImpl {

        protected abstract long currentTimeMillis();
    }

    /**
     * Default clock implementation, which is used if no properties are defined.
     */
    private static final class SystemClock extends ClockImpl {

        @Override
        protected long currentTimeMillis() {
            return System.currentTimeMillis();
        }
    }

    /**
     * Clock implementation with static offset, which is used if {@link ClockProperties#HAZELCAST_CLOCK_OFFSET} is defined.
     */
    private static final class SystemOffsetClock extends ClockImpl {

        private final long offset;

        private SystemOffsetClock(final long offset) {
            this.offset = offset;
        }

        @Override
        protected long currentTimeMillis() {
            return System.currentTimeMillis() + offset;
        }
    }
}
