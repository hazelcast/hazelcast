/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import com.hazelcast.internal.nio.ClassLoaderUtil;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

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
 * To use this class properly in unit or integration tests, please have a look at {@code ClockIntegrationTest}.
 */
public final class Clock {

    private static final ClockImpl CLOCK;

    private Clock() {
    }

    /** Returns the current time in ms for the configured {@link ClockImpl} */
    public static long currentTimeMillis() {
        return CLOCK.currentTimeMillis();
    }

    /**
     * Converts from configured clock implementation offset to JVM time offset
     * @param millis
     * @return
     */
    public static long toSystemCurrentTimeMillis(long millis) {
        return CLOCK.toSystemCurrentTimeMillis(millis);
    }

    static {
        CLOCK = createClock();
    }

    static ClockImpl createClock() {
        String clockImplClassName = System.getProperty(ClockProperties.HAZELCAST_CLOCK_IMPL);
        if (clockImplClassName != null) {
            try {
                return ClassLoaderUtil.newInstance(null, clockImplClassName);
            } catch (Exception e) {
                throw rethrow(e);
            }
        }

        String clockOffset = System.getProperty(ClockProperties.HAZELCAST_CLOCK_OFFSET);
        long offset = 0L;
        if (clockOffset != null) {
            try {
                offset = Long.parseLong(clockOffset);
            } catch (NumberFormatException e) {
                throw rethrow(e);
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

        /**
         * This method converts a given custom clock millisecond offset
         * to underlying system's millisecond offset. Default
         * implementation adds the difference between {@link System#currentTimeMillis()}
         * and {@link ClockImpl#currentTimeMillis()} to given millisecond
         * offset. An implementation of this abstract class may choose
         * to override this.
         * @param millis
         * @return
         */
        protected long toSystemCurrentTimeMillis(long millis) {
            return millis + (System.currentTimeMillis() - currentTimeMillis());
        }
    }

    /**
     * Default clock implementation, which is used if no properties are defined. It will return the system time.
     */
    static final class SystemClock extends ClockImpl {

        @Override
        protected long currentTimeMillis() {
            return System.currentTimeMillis();
        }

        @Override
        protected long toSystemCurrentTimeMillis(long millis) {
            return millis;
        }
    }

    /**
     * Clock implementation that returns the system time with a static offset, which is used if
     * {@link ClockProperties#HAZELCAST_CLOCK_OFFSET} is defined.
     */
    static final class SystemOffsetClock extends ClockImpl {

        private final long offset;

        SystemOffsetClock(final long offset) {
            this.offset = offset;
        }

        @Override
        protected long currentTimeMillis() {
            return System.currentTimeMillis() + offset;
        }

        @Override
        protected long toSystemCurrentTimeMillis(long millis) {
            return millis - offset;
        }
    }
}
