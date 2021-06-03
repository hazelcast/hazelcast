/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Defines the names and default values for internal Hazelcast Jet properties.
 *
 * @since Jet 3.2
 */
public final class JetProperties {

    /**
     * Jet will periodically check for new jobs to start and perform cleanup of
     * unused resources. This property configures how often this check and
     * cleanup will be done. Value is in milliseconds.
     *
     * @since Jet 3.2
     */
    public static final HazelcastProperty JOB_SCAN_PERIOD
            = new HazelcastProperty("jet.job.scan.period", SECONDS.toMillis(5), MILLISECONDS);

    /**
     * Maximum number of time in seconds the job results will be kept in
     * the cluster. They will be automatically deleted after this period
     * is reached.
     * <p>
     * Default value is 7 days.
     *
     * @since Jet 3.2
     */
    public static final HazelcastProperty JOB_RESULTS_TTL_SECONDS
            = new HazelcastProperty("jet.job.results.ttl.seconds", DAYS.toSeconds(7), SECONDS);

    /**
     * Maximum number of job results to keep in the cluster, the oldest
     * results will be automatically deleted after this size is reached.
     * <p>
     * Default value is 1,000 jobs.
     *
     * @since Jet 3.2
     */
    public static final HazelcastProperty JOB_RESULTS_MAX_SIZE
            = new HazelcastProperty("jet.job.results.max.size", 1_000);

    /**
     * Root of Jet installation. Used as default location for the lossless
     * restart store. By default it will be automatically set to the start of
     * the Jet installation path.
     *
     * @since Jet 3.2
     */
    public static final HazelcastProperty JET_HOME
            = new HazelcastProperty("jet.home", "");

    /**
     * Hazelcast Jet normally checks that the version of IMDG on the classpath
     * matches exactly the version it is built for, and fails on mismatch.
     * Setting this property to {@code true} allows Jet to start up even on
     * mismatch. This may be helpful if the user needs some slight IMDG version
     * change (eg. to use a hotfix).
     * <p>
     * <strong>NOTE:</strong> since Jet must read this property at a very early
     * point in startup, it doesn't have an effect when you set it in a
     * Hazelcast configuration file. You must set it as a system property.
     *
     * @since Jet 4.4
     */
    public static final HazelcastProperty JET_IMDG_VERSION_CHECK_DISABLED
            = new HazelcastProperty("jet.imdg.version.mismatch.check.disabled", "false");

    /**
     * The minimum time in microseconds the cooperative worker threads will
     * sleep if none of the tasklets made any progress. Lower values increase
     * idle CPU usage but may result in decreased latency. Higher values will
     * increase latency and very high values (>10000µs) will also limit the
     * throughput.
     * <p>
     * The default is value is {@code 25µs}.
     * <p>
     * Note: the underlying {@link LockSupport#parkNanos(long)} call may
     * actually sleep longer depending on the operating system (up to 15000µs
     * on Windows). See the <a
     * href="https://hazelcast.com/blog/locksupport-parknanos-under-the-hood-and-the-curious-case-of-parking/">
     * Hazelcast blog post about this subject</a> for more details.
     * <p>
     * See also: {@link #JET_IDLE_COOPERATIVE_MAX_MICROSECONDS}
     *
     * @since Jet 3.2
     */
    public static final HazelcastProperty JET_IDLE_COOPERATIVE_MIN_MICROSECONDS
            = new HazelcastProperty("jet.idle.cooperative.min.microseconds", 25, MICROSECONDS);

    /**
     * The maximum time in microseconds the cooperative worker threads will
     * sleep if none of the tasklets made any progress. Lower values increase
     * idle CPU usage but may result in decreased latency. Higher values will
     * increase latency and very high values (>10000µs) will also limit the
     * throughput.
     * <p>
     * The default is value is {@code 500µs}.
     * <p>
     * Note: the underlying {@link LockSupport#parkNanos(long)} call may
     * actually sleep longer depending on the operating system (up to 15000µs on
     * Windows). See the <a
     * href="https://hazelcast.com/blog/locksupport-parknanos-under-the-hood-and-the-curious-case-of-parking/">
     * Hazelcast blog post about this subject</a> for more details.
     * <p>
     * See also: {@link #JET_IDLE_COOPERATIVE_MIN_MICROSECONDS}
     *
     * @since Jet 3.2
     */
    public static final HazelcastProperty JET_IDLE_COOPERATIVE_MAX_MICROSECONDS
        = new HazelcastProperty("jet.idle.cooperative.max.microseconds", 500, MICROSECONDS);

    /**
     * The minimum time in microseconds the non-cooperative worker threads will
     * sleep if none of the tasklets made any progress. Lower values increase
     * idle CPU usage but may result in decreased latency. Higher values will
     * increase latency and very high values (>10000µs) will also limit the
     * throughput.
     * <p>
     * The default is value is {@code 25µs}.
     * <p>
     * Note: the underlying {@link LockSupport#parkNanos(long)} call may actually
     * sleep longer depending on the operating system (up to 15000µs on Windows).
     * See the <a
     * href="https://hazelcast.com/blog/locksupport-parknanos-under-the-hood-and-the-curious-case-of-parking/">
     * Hazelcast blog post about this subject</a> for more details.
     * <p>
     * See also: {@link #JET_IDLE_NONCOOPERATIVE_MAX_MICROSECONDS}
     *
     * @since Jet 3.2
     */
    public static final HazelcastProperty JET_IDLE_NONCOOPERATIVE_MIN_MICROSECONDS
        = new HazelcastProperty("jet.idle.noncooperative.min.microseconds", 25, MICROSECONDS);

    /**
     * The maximum time in microseconds the non-cooperative worker threads will
     * sleep if none of the tasklets made any progress. Lower values increase
     * idle CPU usage but may result in decreased latency. Higher values will
     * increase latency and very high values (>10000µs) will also limit the
     * throughput.
     * <p>
     * The default is value is {@code 5000µs}.
     * <p>
     * Note: the underlying {@link LockSupport#parkNanos(long)} call may actually
     * sleep longer depending on the operating system (up to 15000µs on Windows).
     * See the <a
     * href="https://hazelcast.com/blog/locksupport-parknanos-under-the-hood-and-the-curious-case-of-parking/">
     * Hazelcast blog post about this subject</a> for more details.
     * <p>
     * See also: {@link #JET_IDLE_NONCOOPERATIVE_MIN_MICROSECONDS}
     *
     * @since Jet 3.2
     */
    public static final HazelcastProperty JET_IDLE_NONCOOPERATIVE_MAX_MICROSECONDS
        = new HazelcastProperty("jet.idle.noncooperative.max.microseconds", 5000, MICROSECONDS);

    private JetProperties() {
    }
}
