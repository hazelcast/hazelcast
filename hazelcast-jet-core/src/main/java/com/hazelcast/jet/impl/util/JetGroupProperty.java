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

package com.hazelcast.jet.impl.util;

import com.hazelcast.spi.properties.HazelcastProperty;

import static com.hazelcast.spi.properties.GroupProperty.SHUTDOWNHOOK_ENABLED;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Defines the names and default values for internal Hazelcast Jet properties.
 */
public final class JetGroupProperty {

    public static final HazelcastProperty JOB_SCAN_PERIOD
            = new HazelcastProperty("jet.job.scan.period", SECONDS.toMillis(5), MILLISECONDS);
    public static final HazelcastProperty JET_SHUTDOWNHOOK_ENABLED
            = new HazelcastProperty("jet.shutdownhook.enabled", SHUTDOWNHOOK_ENABLED.getDefaultValue());
    public static final HazelcastProperty JOB_RESULTS_TTL_SECONDS
            = new HazelcastProperty("jet.job.results.ttl.seconds", DAYS.toSeconds(7), SECONDS);

    /**
     * The minimum time in microseconds the worker threads will sleep if none
     * of the tasklets made any progress. If you see high cpu usage for jobs
     * with otherwise low traffic, you can try increasing the value.
     * <p>
     * The default value is 25µs, but the {@code parkNano} call actually sleeps
     * longer, by default 50µs on Linux and up to 15000µs on Windows. See
     * https://hazelcast.com/blog/locksupport-parknanos-under-the-hood-and-the-curious-case-of-parking/
     * for more information. Higher value also slightly increases latency and a
     * very high value (>10000µs) limits the throughput.
     */
    public static final HazelcastProperty JET_MINIMUM_IDLE_MICROSECONDS
            = new HazelcastProperty("jet.minimum.idle.microseconds", 25, MICROSECONDS);

    private JetGroupProperty() {
    }
}
