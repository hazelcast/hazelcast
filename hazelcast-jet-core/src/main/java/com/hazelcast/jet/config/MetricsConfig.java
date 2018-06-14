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

package com.hazelcast.jet.config;

import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;

/**
 * Configuration options specific to metrics collection.
 */
public class MetricsConfig {

    /**
     * Default collection interval for metrics
     */
    public static final int DEFAULT_METRICS_COLLECTION_SECONDS = 5;

    /**
     * Default retention period for metrics.
     */
    public static final int DEFAULT_METRICS_RETENTION_SECONDS = 5;

    private boolean enabled = true;
    private int retentionSeconds = DEFAULT_METRICS_RETENTION_SECONDS;
    private boolean enableDataStructures;
    private int intervalSeconds = DEFAULT_METRICS_COLLECTION_SECONDS;

    /**
     * Sets whether metrics collection should be enabled for the node. It's
     * enabled by default.
     */
    @Nonnull
    public MetricsConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Returns if metrics collection is enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Returns the number of seconds the metrics will be retained on the
     * instance. By default metrics are retained for 5 seconds.
     */
    @Nonnull
    public MetricsConfig setRetentionSeconds(int retentionSeconds) {
        Preconditions.checkPositive(intervalSeconds, "retentionSeconds must be positive");
        this.retentionSeconds = retentionSeconds;
        return this;
    }

    /**
     * Returns the number of seconds the metrics will be retained on the
     * instance.
     */
    public int getRetentionSeconds() {
        return retentionSeconds;
    }

    /**
     * Sets the metrics collection interval in seconds. By default, metrics
     * are collected every 5 seconds.
     */
    @Nonnull
    public MetricsConfig setCollectionIntervalSeconds(int intervalSeconds) {
        Preconditions.checkPositive(intervalSeconds, "intervalSeconds must be positive");
        this.intervalSeconds = intervalSeconds;
        return this;
    }

    /**
     * Returns the metrics collection interval.
     */
    public int getCollectionIntervalSeconds() {
        return this.intervalSeconds;
    }

    /**
     * Sets whether metrics should be collected for data structures. Metrics
     * collection can have some overhead if there is a large number of data
     * structures. It's disabled by default.
     */
    @Nonnull
    public MetricsConfig setEnabledForDataStructures(boolean enableDataStructures) {
        this.enableDataStructures = enableDataStructures;
        return this;
    }

    /**
     * Returns if metric collection is enabled for data structures.
     */
    public boolean isEnabledForDataStructures() {
        return enableDataStructures;
    }

}
