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

package com.hazelcast.test.bounce;

import com.hazelcast.config.Config;

/**
 * Configuration for a member bounce test.
 */
public class BounceTestConfiguration {

    private final int clusterSize;
    private final DriverType driverType;
    private final Config memberConfig;
    private final int driverCount;
    private final DriverFactory driverFactory;
    private final boolean useTerminate;
    private final int bouncingIntervalSeconds;
    private final long maximumStaleSeconds;

    /**
     * Indicates whether the test will be driven by member or client HazelcastInstances
     */
    public enum DriverType {
        /**
         * Use the single member that is never shutdown during the test as test driver
         */
        ALWAYS_UP_MEMBER,
        /**
         * Setup separate members as test drivers
         */
        MEMBER,
        /**
         * Setup clients as test drivers
         */
        CLIENT,
    }

    BounceTestConfiguration(int clusterSize, DriverType driverType,
                            Config memberConfig, int driverCount, DriverFactory driverFactory, boolean useTerminate,
                            int bouncingIntervalSeconds, long maximumStaleSeconds) {
        this.clusterSize = clusterSize;
        this.driverType = driverType;
        this.memberConfig = memberConfig;
        this.driverCount = driverCount;
        this.driverFactory = driverFactory;
        this.useTerminate = useTerminate;
        this.bouncingIntervalSeconds = bouncingIntervalSeconds;
        this.maximumStaleSeconds = maximumStaleSeconds;
    }

    public int getClusterSize() {
        return clusterSize;
    }

    public DriverType getDriverType() {
        return driverType;
    }

    public Config getMemberConfig() {
        return memberConfig;
    }

    public int getDriverCount() {
        return driverCount;
    }

    public DriverFactory getDriverFactory() {
        return driverFactory;
    }

    public boolean isUseTerminate() {
        return useTerminate;
    }

    public int getBouncingIntervalSeconds() {
        return bouncingIntervalSeconds;
    }

    public long getMaximumStaleSeconds() {
        return maximumStaleSeconds;
    }
}
