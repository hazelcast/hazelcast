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

package com.hazelcast.client.config;

/**
 * Configuration for icmp ping failure detector of clients.
 * ICMP ping is used to detect if machine that a remote hazelcast member runs on alive or not
 * ICMP Ping detector will attempt {@link #maxAttempts} times, one every {@link #intervalMilliseconds}.
 * and will wait up-to {@link #timeoutMilliseconds}. for each to complete.
 * In each attempt icmp will go through maximum {@link #ttl} hops.
 * If, after {@link #maxAttempts}. are made , there was no successful ping, the member will get suspected.
 */
public class ClientIcmpPingConfig {

    /**
     * Default timeout for icmp detection in millis see {@link #timeoutMilliseconds}
     */
    public static final int DEFAULT_TIMEOUT_MILLISECONDS = 1000;

    /**
     * Default interval between icmp ping attempts see {@link #intervalMilliseconds}
     */
    public static final int DEFAULT_INTERVAL_MILLISECONDS = 1000;

    /**
     * Default ttl for icmp packages {@link #ttl}.
     */
    public static final int DEFAULT_TTL = 255;

    /**
     * Default max number of attempts {@link #maxAttempts}
     */
    public static final int DEFAULT_MAX_ATTEMPT = 2;

    /**
     * Duration to wait for the response of single icmp package in milliseconds
     * This cannot be more than the interval value. Should always be smaller.
     */
    private int timeoutMilliseconds = DEFAULT_TIMEOUT_MILLISECONDS;

    /**
     * Default value is 1000 milliseconds.
     * This value can not be smaller than 1000 milliseconds
     */
    private int intervalMilliseconds = DEFAULT_INTERVAL_MILLISECONDS;

    /**
     * Flag to enable fail fast behaviour of client icmp
     * When `echoFailFastOnStartup` is set to `true`, If OS is not supported,
     * or not configured correctly as per reference-manual, hazelcast-client will fail to start.
     */
    private boolean echoFailFastOnStartup = true;

    /**
     * the maximum number of hops the packets should go through before giving up as failure.
     */
    private int ttl = DEFAULT_TTL;

    /**
     * Max ping attempts before suspecting a member
     */
    private int maxAttempts = DEFAULT_MAX_ATTEMPT;

    /**
     * Flag to enable icmp ping failure detector
     * When set to true, this client will use icmp ping failure detector to detect unavailable members
     */
    private boolean enabled;

    public ClientIcmpPingConfig() {
    }

    public ClientIcmpPingConfig(ClientIcmpPingConfig config) {
        timeoutMilliseconds = config.timeoutMilliseconds;
        intervalMilliseconds = config.intervalMilliseconds;
        echoFailFastOnStartup = config.echoFailFastOnStartup;
        ttl = config.ttl;
        maxAttempts = config.maxAttempts;
        enabled = config.enabled;
    }

    public int getTimeoutMilliseconds() {
        return timeoutMilliseconds;
    }

    public ClientIcmpPingConfig setTimeoutMilliseconds(int timeoutMilliseconds) {
        this.timeoutMilliseconds = timeoutMilliseconds;
        return this;
    }

    public int getIntervalMilliseconds() {
        return intervalMilliseconds;
    }

    public ClientIcmpPingConfig setIntervalMilliseconds(int intervalMilliseconds) {
        this.intervalMilliseconds = intervalMilliseconds;
        return this;
    }

    public boolean isEchoFailFastOnStartup() {
        return echoFailFastOnStartup;
    }

    public ClientIcmpPingConfig setEchoFailFastOnStartup(boolean echoFailFastOnStartup) {
        this.echoFailFastOnStartup = echoFailFastOnStartup;
        return this;
    }

    public int getTtl() {
        return ttl;
    }

    public ClientIcmpPingConfig setTtl(int ttl) {
        this.ttl = ttl;
        return this;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public ClientIcmpPingConfig setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
        return this;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public ClientIcmpPingConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity"})
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClientIcmpPingConfig that = (ClientIcmpPingConfig) o;

        if (timeoutMilliseconds != that.timeoutMilliseconds) {
            return false;
        }
        if (intervalMilliseconds != that.intervalMilliseconds) {
            return false;
        }
        if (echoFailFastOnStartup != that.echoFailFastOnStartup) {
            return false;
        }
        if (ttl != that.ttl) {
            return false;
        }
        if (maxAttempts != that.maxAttempts) {
            return false;
        }
        return enabled == that.enabled;
    }

    @Override
    public int hashCode() {
        int result = timeoutMilliseconds;
        result = 31 * result + intervalMilliseconds;
        result = 31 * result + (echoFailFastOnStartup ? 1 : 0);
        result = 31 * result + ttl;
        result = 31 * result + maxAttempts;
        result = 31 * result + (enabled ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ClientIcmpPingConfig{"
                + "enabled=" + enabled
                + ", timeoutMilliseconds=" + timeoutMilliseconds
                + ", intervalMilliseconds=" + intervalMilliseconds
                + ", echoFailFastOnStartup=" + echoFailFastOnStartup
                + ", ttl=" + ttl
                + ", maxAttempts=" + maxAttempts
                + '}';
    }
}
