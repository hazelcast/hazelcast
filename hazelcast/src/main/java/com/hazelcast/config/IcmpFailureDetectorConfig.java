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

package com.hazelcast.config;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static java.lang.String.format;

/**
 * This Failure Detector may be configured in addition to one of Deadline and Phi Accrual Failure Detectors.<br>
 * It operates at Layer 3 of the OSI protocol, and provides much quicker and more deterministic detection of hardware<br>
 * and other lower level events. This detector may be configured to perform an extra check after a member is suspected by one<br>
 * of the other detectors, or it can work in parallel, which is the default. This way hardware and network level issues<br>
 * will be detected more quickly.
 *
 * <p>This failure detector is based on InetAddress.isReachable().
 * When the JVM process has enough permissions to create RAW sockets, the implementation will choose to rely on
 * ICMP Echo requests. This is preferred.
 * However, if there are not enough permissions, it can be configured to fallback on attempting a TCP Echo on port 7.
 * In the latter case, both a successful connection or an explicit rejection will be treated as "Host is Reachable".
 * This is not preferred as each call creates a heavyweight socket and moreover the Echo service is typically disabled.
 *
 * <p>For the Ping Failure Detector to rely only on ICMP (RAW sockets) Echo requests, there are some criteria
 * that need to be met. Please consult the Hazelcast Reference Manual on how to configure and what the OS requirements are.
 */
public class IcmpFailureDetectorConfig {

    /**
     * Default timeout for detection in millis see {@link #timeoutMilliseconds}
     */
    private static final int DEFAULT_TIMEOUT_MILLISECONDS = 1000;

    /**
     * Default interval between ping attempts see {@link #intervalMilliseconds}
     */
    private static final int DEFAULT_INTERVAL_MILLISECONDS = 1000;

    private static final int MIN_INTERVAL_MILLIS = 1000;

    /**
     * Default ttl for ping packets {@link #ttl}.
     */
    private static final int DEFAULT_TTL = 255;

    /**
     * Default max number of attempts {@link #maxAttempts}
     */
    private static final int DEFAULT_MAX_ATTEMPT = 2;

    private static final boolean DEFAULT_ENABLED = false;

    private static final boolean DEFAULT_PARALLEL_MODE = true;

    private int timeoutMilliseconds = DEFAULT_TIMEOUT_MILLISECONDS;

    private int intervalMilliseconds = DEFAULT_INTERVAL_MILLISECONDS;

    private boolean failFastOnStartup = true;

    private int ttl = DEFAULT_TTL;

    private int maxAttempts = DEFAULT_MAX_ATTEMPT;

    private boolean enabled = DEFAULT_ENABLED;

    private boolean parallelMode = DEFAULT_PARALLEL_MODE;

    /**
     * @return the timeout in Milliseconds before declaring a failed ping
     */
    public int getTimeoutMilliseconds() {
        return timeoutMilliseconds;
    }

    /**
     * Sets the timeout in Milliseconds before declaring a failed ping
     * This cannot be more than the interval value. Should always be smaller.
     *
     * @param timeoutMilliseconds the timeout for each ping in millis
     * @return this {@link IcmpFailureDetectorConfig} instance
     */
    public IcmpFailureDetectorConfig setTimeoutMilliseconds(int timeoutMilliseconds) {
        checkPositive("timeoutMilliseconds", timeoutMilliseconds);
        this.timeoutMilliseconds = timeoutMilliseconds;
        return this;
    }

    /**
     * @return the time in milliseconds between each ping
     */
    public int getIntervalMilliseconds() {
        return intervalMilliseconds;
    }

    /**
     * Sets the time in milliseconds between each ping
     * This value can not be smaller than 1000 milliseconds
     *
     * @param intervalMilliseconds the interval millis between each ping
     * @return this {@link IcmpFailureDetectorConfig} instance
     */
    public IcmpFailureDetectorConfig setIntervalMilliseconds(int intervalMilliseconds) {
        if (intervalMilliseconds < MIN_INTERVAL_MILLIS) {
            throw new InvalidConfigurationException(format("Interval can't be set to less than %d milliseconds.",
                    MIN_INTERVAL_MILLIS));
        }

        this.intervalMilliseconds = intervalMilliseconds;
        return this;
    }

    /**
     * @return whether the member should fail-fast on startup if ICMP is not allowed by the underlying OS.
     */
    public boolean isFailFastOnStartup() {
        return failFastOnStartup;
    }

    /**
     * Sets whether the member should fail-fast on startup if ICMP is not allowed by the underlying OS.
     * Failure is usually due to OS level restrictions.
     *
     * @param failFastOnStartup the fail-fast flag
     * @return this {@link IcmpFailureDetectorConfig} instance
     */
    public IcmpFailureDetectorConfig setFailFastOnStartup(boolean failFastOnStartup) {
        this.failFastOnStartup = failFastOnStartup;
        return this;
    }

    /**
     * @return the maximum number of times the IP Datagram (ping) can be forwarded
     */
    public int getTtl() {
        return ttl;
    }

    /**
     * Sets the maximum number of times the IP Datagram (ping) can be forwarded, in most cases
     * all Hazelcast cluster members would be within one network switch/router therefore
     *
     * @param ttl the new time-to-live value
     * @return this {@link IcmpFailureDetectorConfig} instance
     */
    public IcmpFailureDetectorConfig setTtl(int ttl) {
        checkNotNegative(ttl, "TTL must not be a negative value");
        this.ttl = ttl;
        return this;
    }

    /**
     * @return max ping attempts before suspecting a member
     */
    public int getMaxAttempts() {
        return maxAttempts;
    }

    /**
     * Set the max ping attempts before suspecting a member
     *
     * @param maxAttempts the max attempts before suspecting a member
     * @return this {@link IcmpFailureDetectorConfig} instance
     */
    public IcmpFailureDetectorConfig setMaxAttempts(int maxAttempts) {
        checkNotNegative(maxAttempts, "Max attempts must not be a negative value");
        this.maxAttempts = maxAttempts;
        return this;
    }

    /**
     * @return whether the ICMP detector is enabled or not.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Set whether the ICMP failure detector should be enabled or not.
     * When set to true, this member will use icmp ping failure detector to detect unavailable members
     */
    public IcmpFailureDetectorConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * @return whether the ICMP detector is set in parallel mode, or in legacy (flag value = <code>false</code>).
     */
    public boolean isParallelMode() {
        return parallelMode;
    }

    /**
     * Set the ICMP detector to run in parallel mode, independent from the other failure detectors.
     * If set to false (default) then the detector runs in legacy mode, with similar behavior as to Hazelcast version less than
     * 3.9.
     *
     * @param mode the mode representing whether the detector should work in parallel or not
     * @return this {@link IcmpFailureDetectorConfig} instance
     */
    public IcmpFailureDetectorConfig setParallelMode(boolean mode) {
        this.parallelMode = mode;
        return this;
    }

    @Override
    public String toString() {
        return "IcmpFailureDetectorConfig{"
                + "timeoutMilliseconds=" + timeoutMilliseconds
                + ", intervalMilliseconds=" + intervalMilliseconds
                + ", echoFailFastOnStartup=" + failFastOnStartup
                + ", ttl=" + ttl
                + ", maxAttempts=" + maxAttempts
                + ", enabled=" + enabled
                + ", parallelMode=" + parallelMode
                + '}';
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof IcmpFailureDetectorConfig)) {
            return false;
        }

        IcmpFailureDetectorConfig that = (IcmpFailureDetectorConfig) o;

        if (timeoutMilliseconds != that.timeoutMilliseconds) {
            return false;
        }
        if (intervalMilliseconds != that.intervalMilliseconds) {
            return false;
        }
        if (failFastOnStartup != that.failFastOnStartup) {
            return false;
        }
        if (ttl != that.ttl) {
            return false;
        }
        if (maxAttempts != that.maxAttempts) {
            return false;
        }
        if (enabled != that.enabled) {
            return false;
        }
        return parallelMode == that.parallelMode;
    }

    @Override
    public final int hashCode() {
        int result = timeoutMilliseconds;
        result = 31 * result + intervalMilliseconds;
        result = 31 * result + (failFastOnStartup ? 1 : 0);
        result = 31 * result + ttl;
        result = 31 * result + maxAttempts;
        result = 31 * result + (enabled ? 1 : 0);
        result = 31 * result + (parallelMode ? 1 : 0);
        return result;
    }
}
