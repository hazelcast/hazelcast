/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import java.util.function.Supplier;

/**
 * Configuration for a member bounce test.
 */
public class BounceTestConfiguration {

    private final int clusterSize;
    private final Supplier<Config> memberConfigSupplier;
    private final DriverConfiguration driverConfig;
    private final boolean useTerminate;
    private final boolean avoidOverlappingTerminations;
    private final int bouncingIntervalSeconds;
    private final long maximumStaleSeconds;
    private final boolean hasSteadyMember;
    private final boolean shouldFailOnIncompleteTask;
    private final int testTaskTimeoutSecs;

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
         * Setup separate lite members as test drivers. Lite members do not affect
         * partition distribution.
         */
        LITE_MEMBER,
        /**
         * Setup clients as test drivers
         */
        CLIENT,
    }

    record DriverConfiguration(DriverType type, int count, DriverFactory factory) {
    }

    BounceTestConfiguration(int clusterSize, DriverConfiguration driverConfig, Supplier<Config> memberConfigSupplier,
                            boolean useTerminate, boolean avoidOverlappingTerminations, int bouncingIntervalSeconds,
                            long maximumStaleSeconds, boolean hasSteadyMember, boolean shouldFailOnIncompleteTask,
                            int testTaskTimeoutSecs) {
        this.clusterSize = clusterSize;
        this.driverConfig = driverConfig;
        this.memberConfigSupplier = memberConfigSupplier;
        this.useTerminate = useTerminate;
        this.avoidOverlappingTerminations = avoidOverlappingTerminations;
        this.bouncingIntervalSeconds = bouncingIntervalSeconds;
        this.maximumStaleSeconds = maximumStaleSeconds;
        this.hasSteadyMember = hasSteadyMember;
        this.shouldFailOnIncompleteTask = shouldFailOnIncompleteTask;
        this.testTaskTimeoutSecs = testTaskTimeoutSecs;
    }

    public int getClusterSize() {
        return clusterSize;
    }

    public DriverType getDriverType() {
        return driverConfig.type();
    }

    public Supplier<Config> getMemberConfigSupplier() {
        return memberConfigSupplier;
    }

    public int getDriverCount() {
        return driverConfig.count;
    }

    public DriverFactory getDriverFactory() {
        return driverConfig.factory();
    }

    public boolean isUseTerminate() {
        return useTerminate;
    }

    public boolean avoidOverlappingTerminations() {
        return avoidOverlappingTerminations;
    }

    public int getBouncingIntervalSeconds() {
        return bouncingIntervalSeconds;
    }

    public long getMaximumStaleSeconds() {
        return maximumStaleSeconds;
    }

    public boolean hasSteadyMember() {
        return hasSteadyMember;
    }

    public boolean shouldFailOnIncompleteTask() {
        return shouldFailOnIncompleteTask;
    }

    public int getTestTaskTimeoutSecs() {
        return testTaskTimeoutSecs;
    }
}
