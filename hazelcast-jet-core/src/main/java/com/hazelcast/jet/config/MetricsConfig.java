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

package com.hazelcast.jet.config;

import com.hazelcast.internal.diagnostics.Diagnostics;
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
    private boolean jmxEnabled = true;
    private int retentionSeconds = DEFAULT_METRICS_RETENTION_SECONDS;
    private boolean metricsForDataStructuresEnabled;
    private int intervalSeconds = DEFAULT_METRICS_COLLECTION_SECONDS;

    /**
     * Sets whether metrics collection should be enabled for the node. If
     * enabled, Hazelcast Jet Management Center will be able to connect to this
     * member. It's enabled by default.
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
     * Returns whether metrics will be exposed through JMX MBeans.
     */
    public boolean isJmxEnabled() {
        return jmxEnabled;
    }

    /**
     * Enables metrics exposure through JMX. It's enabled by default. Metric
     * values are collected in the {@linkplain #setCollectionIntervalSeconds
     * metric collection interval} and written to a set of MBeans.
     * <p>
     * Metrics are exposed by converting the metric name to a structure that
     * will be rendered in a tree structure in tools showing the beans (Java
     * Mission Control or JConsole). MBean is identified by a {@link
     * javax.management.ObjectName} which contains tag-value tuples. Although
     * the order of tags is insignificant to the identity, the UI tools use the
     * order of tags to generate tree structure and only display the values,
     * not the tag names. Metric names also have tags and values, but in order
     * to display the tag name in the UI, we use {@code "tag=value"} as a
     * value. For example, the metric:
     *
     * <pre>
     *     [module=jet,job=123,vertex=a,metric=emittedCount]
     * </pre>
     *
     * will be rendered as an attribute named {@code emittedCount} in an MBean
     * identified by:
     *
     * <pre>{@code
     *     com.hazelcast.jet:type=Metrics,instance=<instanceName>,tag0="job=123",tag1="vertex=a"
     * }</pre>
     *
     * This will cause the MBean to be rendered in a tree under {@code
     * com.hazelcast.jet/Metrics/<instanceName>/job=123/vertex=a} nodes. For
     * the old-style metric names, the name will be split on dots ('.') and a
     * tag will be created for each level. The element after the last dot will
     * be used as metric name.
     * <p>
     * Also some other tags are treated specially, namely {@code unit} (it is
     * copied to attribute description), {@code module} (it is appended to the
     * domain) and {@code metric} (it will be converted to attribute name in
     * the MBean).
     */
    public MetricsConfig setJmxEnabled(boolean jmxEnabled) {
        this.jmxEnabled = jmxEnabled;
        return this;
    }

    /**
     * Returns the number of seconds the metrics will be retained on the
     * instance. By default, metrics are retained for 5 seconds (that is for
     * one collection of metrics values, if default {@linkplain
     * #setCollectionIntervalSeconds(int) collection interval} is used). More
     * retention means more heap memory, but allows for longer client hiccups
     * without losing a value (for example to restart the Management Center).
     * <p>
     * This setting applies only to Jet Management Center metrics API. It
     * doesn't affect how metrics are exposed through JMX.
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
     * Sets the metrics collection interval in seconds. The same interval is
     * used for collection for Management Center and for JMX publisher. By default,
     * metrics are collected every 5 seconds.
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
     * Sets whether statistics for data structures are added to metrics. See
     * {@link Diagnostics#METRICS_DISTRIBUTED_DATASTRUCTURES}. It's disabled by
     * default.
     */
    @Nonnull
    public MetricsConfig setMetricsForDataStructuresEnabled(boolean metricsForDataStructuresEnabled) {
        this.metricsForDataStructuresEnabled = metricsForDataStructuresEnabled;
        return this;
    }

    /**
     * Returns if statistics for data structures are added to metrics. See
     * {@link Diagnostics#METRICS_DISTRIBUTED_DATASTRUCTURES}.
     */
    public boolean isMetricsForDataStructuresEnabled() {
        return metricsForDataStructuresEnabled;
    }
}
