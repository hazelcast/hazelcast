/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
 *  Utility class to be able to simulate different time zones.
 *  Time offset can be configured with the property <code>com.hazelcast.clock.offset</code>
 */
public final class Clock {

    /**
     * Clock offset property in milliseconds. When it is set to a non-zero value,
     * {@link Clock#currentTimeMillis()} will return a shifted {@link System#currentTimeMillis()}
     * time by the given offset value.
     */
    public static final String HAZELCAST_CLOCK_OFFSET = "com.hazelcast.clock.offset";

    /**
     * Classname of a {@link com.hazelcast.util.Clock.ClockImpl} implementation.
     * When this property is set, {@link Clock#currentTimeMillis()}
     * will call the <code>currentTimeMillis()</code> method of given <code>ClockImpl</code>.
     */
    public static final String HAZELCAST_CLOCK_IMPL = "com.hazelcast.clock.impl";

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
        String clockImplClassName = System.getProperty(HAZELCAST_CLOCK_IMPL);
        if (clockImplClassName != null) {
            try {
                return ClassLoaderUtil.newInstance(null, clockImplClassName);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }

        String clockOffset = System.getProperty(HAZELCAST_CLOCK_OFFSET);
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
     * Clock abstraction to be able to simulate different clocks
     * without changing actual system time.
     */
    public abstract static class ClockImpl {

        protected abstract long currentTimeMillis();
    }

    private static final class SystemClock extends ClockImpl {

        @Override
        protected long currentTimeMillis() {
            return System.currentTimeMillis();
        }

        @Override
        public String toString() {
            return "SystemClock";
        }
    }

    private static final class SystemOffsetClock extends ClockImpl {

        private final long offset;

        private SystemOffsetClock(final long offset) {
            this.offset = offset;
        }

        @Override
        protected long currentTimeMillis() {
            return System.currentTimeMillis() + offset;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("SystemOffsetClock");
            sb.append("{offset=").append(offset);
            sb.append('}');
            return sb.toString();
        }
    }


}
