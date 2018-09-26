/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.jitter;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import static com.hazelcast.test.HazelcastTestSupport.sleepAtLeastMillis;
import static com.hazelcast.test.JenkinsDetector.isOnJenkins;
import static com.hazelcast.util.QuickMath.nextPowerOfTwo;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * JUnit rule for detecting JVM/OS hiccups. It's meant to give you an insight into your environment
 * in the case of a test failure. This is useful for troubleshooting of spuriously failing tests.
 */
@SuppressWarnings("WeakerAccess")
public class JitterRule implements TestRule {

    /**
     * Time interval aggregated into a single bucket. Smaller interval provides
     * a clearer picture about hiccups in time, too small intervals may use too
     * much of memory and can also generate overwhelming amount of data.
     */
    public static final long AGGREGATION_INTERVAL_MILLIS = SECONDS.toMillis(5);

    /**
     * Number of buckets to be created. Jitter monitor records a floating window
     * where the length of the window can be calculated as
     * {@code AGGREGATION_INTERVAL_MILLIS * CAPACITY}
     * <p>
     * It has to be a power of two.
     */
    public static final int CAPACITY = nextPowerOfTwo(720);

    /**
     * Resolution of the measurement. Smaller number can detect shorter pauses,
     * but it can cause too much of overhead. Too long value causes less overhead,
     * but it may miss shorter pauses.
     */
    public static final long RESOLUTION_NANOS = MILLISECONDS.toNanos(10);

    /**
     * Hiccups over this threshold will be counted separately. This is useful for counting
     * serious hiccups.
     */
    public static final long LONG_HICCUP_THRESHOLD = SECONDS.toNanos(1);

    private static final String MODE_PROPERTY_NAME = "hazelcast.jitterMonitor.mode";
    private static final Mode DEFAULT_MODE = Mode.JENKINS;
    private static final boolean ENABLED = isEnabled();

    static {
        if (ENABLED) {
            JitterMonitor.ensureRunning();
        }
    }

    private static boolean isEnabled() {
        String modePropertyValue = System.getProperty(MODE_PROPERTY_NAME, DEFAULT_MODE.name());
        Mode mode = Mode.valueOf(modePropertyValue);
        switch (mode) {
            case DISABLED:
                return false;
            case ENABLED:
                return true;
            case JENKINS:
                return isOnJenkins();
            default:
                throw new IllegalArgumentException("Unknown mode: " + mode);
        }
    }

    @Override
    public Statement apply(final Statement base, final Description description) {
        if (!ENABLED) {
            return base;
        }

        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                int retries = 0;
                long maxHiccupThresholdNanos = 0;
                long coolDownPeriodMs = 0;

                RetryableUponHiccups retryable = description.getAnnotation(RetryableUponHiccups.class);
                if (retryable != null) {
                    retries = retryable.retries();
                    maxHiccupThresholdNanos = MILLISECONDS.toNanos(retryable.thresholdMs());
                    coolDownPeriodMs = retryable.coolDownPeriodMs();

                }

                boolean shouldRetry = false;
                int run = 1;
                do {
                    long startTime = System.currentTimeMillis();
                    try {
                        base.evaluate();
                    } catch (Throwable t) {
                        long endTime = System.currentTimeMillis();
                        Iterable<Slot> slots = JitterMonitor.getSlotsBetween(startTime, endTime);
                        printJitters(slots, run);
                        shouldRetry = retries > 0 && hiccupsDetected(slots, maxHiccupThresholdNanos);

                        if (!shouldRetry || run >= retries) {
                            throw t;
                        } else {
                            run++;
                            printException(t);
                            sleepAtLeastMillis(coolDownPeriodMs);
                            System.out.println("Started Running Test: " + description.getMethodName() + ". Attempt: " + run);
                        }
                    }
                } while (shouldRetry);
            }

            private boolean hiccupsDetected(Iterable<Slot> slots, long maxHiccupThreshold) {
                for (Slot slot : slots) {
                    if (slot.getAccumulatedHiccupsNanos() >= maxHiccupThreshold) {
                        return true;
                    }
                }

                return false;
            }

            private void printException(Throwable t) {
                StringWriter out = new StringWriter();
                t.printStackTrace(new PrintWriter(out));
                System.err.println(out);
            }

            private void printJitters(Iterable<Slot> slots, int currentTry) {
                StringBuilder sb = new StringBuilder("Hiccups measured while running test '")
                        .append(description.getDisplayName())
                        .append("' attempt: ")
                        .append(currentTry)
                        .append(LINE_SEPARATOR);
                DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
                for (Slot slot : slots) {
                    sb.append(slot.toHumanFriendly(dateFormat)).append(LINE_SEPARATOR);
                }
                System.out.println(sb);
            }
        };
    }
}
