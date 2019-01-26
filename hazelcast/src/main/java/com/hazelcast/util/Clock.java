/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import java.io.File;

import static com.hazelcast.nio.IOUtil.fileAsText;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Abstracts the system clock to simulate different clocks without changing the actual system time.
 * <p>
 * Can be used to simulate different time zones or to control timing related behavior in Hazelcast.
 * <p>
 * The time offset can be configured with the property {@value ClockProperties#HAZELCAST_CLOCK_OFFSET}.
 * The clock implementation can be configured with the property {@value ClockProperties#HAZELCAST_CLOCK_IMPL}.
 *
 * <b>WARNING:</b> This class is a singleton.
 * Once the class has been initialized, the clock implementation or offset cannot be changed.
 * To use this class properly in unit or integration tests, please have a look at {@code ClockIntegrationTest}.
 */
public final class Clock {

    private static final ClockImpl CLOCK;
    // this is an experimental setting. Should not be used for production.
    private static final boolean XEN_CLOCK_ENABLED
            = Boolean.parseBoolean(System.getProperty("hazelcast.xenclock", "false"));
    private static final int XEN_CLOCK_UPDATE_PERIOD_MS
            = Integer.getInteger("hazelcast.xenclock.updateperiod.millis", 100);

    private Clock() {
    }

    /**
     * Returns the current time in ms for the configured {@link ClockImpl}
     */
    public static long currentTimeMillis() {
        return CLOCK.currentTimeMillis();
    }

    public static long approximateTimeMillis() {
        return CLOCK.approximateTimeMillis();
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

        if (XEN_CLOCK_ENABLED && XenOptimizedClock.xenClockDetected()) {
            return new XenOptimizedClock();
        } else {
            return new SystemClock();
        }
    }


    /**
     * Extend this class if you want to provide your own clock implementation.
     */
    public abstract static class ClockImpl {

        protected abstract long currentTimeMillis();

        protected abstract long approximateTimeMillis();
    }

    /**
     * Xen optimized clock.
     */
    public static class XenOptimizedClock extends ClockImpl {

        private volatile long approximateTimeMillis = System.currentTimeMillis();

        XenOptimizedClock() {
            new ClockFetchThread().start();
            System.out.println("Xen clocksource detected");
        }

        protected long currentTimeMillis() {
            return System.currentTimeMillis();
        }

        protected long approximateTimeMillis() {
            return approximateTimeMillis;
        }

        private class ClockFetchThread extends Thread {
            ClockFetchThread() {
                super("HZ-XenClockUpdater");
                setDaemon(true);
            }

            @Override
            public void run() {
                while (true) {
                    approximateTimeMillis = System.currentTimeMillis();
                    try {
                        Thread.sleep(XEN_CLOCK_UPDATE_PERIOD_MS);
                    } catch (InterruptedException e) {
                        EmptyStatement.ignore(e);
                    }
                }
            }
        }

        private static boolean xenClockDetected() {
            File file = new File("/sys/devices/system/clocksource/clocksource0/current_clocksource");
            if (!file.exists()) {
                System.out.println("/sys/devices/system/clocksource/clocksource0/current_clocksource does not exist");
                return false;
            }

            try {
                String clock = fileAsText(file);
                return clock.toLowerCase().contains("xen");
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
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
        protected long approximateTimeMillis() {
            return System.currentTimeMillis();
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
        protected long approximateTimeMillis() {
            return currentTimeMillis();
        }
    }
}
