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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.nio.charset.Charset.forName;
import static org.codehaus.groovy.runtime.DefaultGroovyMethodsSupport.closeQuietly;

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

    private Clock() {
    }

    /**
     * Returns the current time in ms for the configured {@link ClockImpl}
     */
    public static long currentTimeMillis() {
        return CLOCK.currentTimeMillis();
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

        return new XenOptimizedClock();
    }



    /**
     * Extend this class if you want to provide your own clock implementation.
     */
    public abstract static class ClockImpl {

        protected abstract long currentTimeMillis();
    }

    /**
     * Default clock implementation, which is used if no properties are defined. It will return the system time.
     */
    static final class SystemClock extends ClockImpl {

        @Override
        protected long currentTimeMillis() {
            return System.currentTimeMillis();
        }
    }

    /**
     * Makes use of nanotime instead of System.currentTimeMillis()
     *
     * The problem is that in EC2 using XEN the currentTimeMillis VDSO doesn't
     * work correctly and therefor a system call is performed to read the time.
     *
     * https://heapanalytics.com/blog/engineering/clocksource-aws-ec2-vdso
     */
    static final class XenOptimizedClock extends ClockImpl {

        private final long startNanos;
        private final long startMillis;
        private final long NANOS_PER_MS = TimeUnit.MILLISECONDS.toNanos(1);

        private XenOptimizedClock() {
            System.out.println("Using XenOptimizedClock");

            for (; ; ) {
                long t1 = System.currentTimeMillis();
                long nanos = System.nanoTime();
                long t2 = System.currentTimeMillis();

                if (t1 == t2) {
                    startNanos = nanos;
                    startMillis = t1;
                    break;
                }
            }
        }

        @Override
        protected long currentTimeMillis() {
            long elapsedNanos = System.nanoTime() - startNanos;
            return startMillis + elapsedNanos / NANOS_PER_MS;
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
    }
}
