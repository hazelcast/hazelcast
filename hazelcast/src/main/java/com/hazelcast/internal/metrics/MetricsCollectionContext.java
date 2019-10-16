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

package com.hazelcast.internal.metrics;

/**
 * An interface that extracts the metrics from the objects collected by
 * the {@link DynamicMetricsProvider} implementations.
 */
public interface MetricsCollectionContext {
    /**
     * Extracts, tags and collects all metrics from the given source.
     *
     * @param metricTagger The {@link MetricTagger} used to tag the metrics
     *                     extracted from the {@code source} object
     * @param source       The object that contains the metrics
     */
    void collect(MetricTagger metricTagger, Object source);

    /**
     * Collects the given metric.
     *
     * @param tagger The {@link MetricTagger} used to provide the tags
     *               for the collected metric
     * @param name   The name of the metric
     * @param level  The level
     * @param value  The value of the collected metric
     */
    void collect(MetricTagger tagger, String name, ProbeLevel level, ProbeUnit unit, long value);

    /**
     * Collects the given metric.
     *
     * @param tagger The {@link MetricTagger} used to provide the tags
     *               for the collected metric
     * @param name   The name of the metric
     * @param level  The level
     * @param value  The value of the collected metric
     */
    void collect(MetricTagger tagger, String name, ProbeLevel level, ProbeUnit unit, double value);

}
