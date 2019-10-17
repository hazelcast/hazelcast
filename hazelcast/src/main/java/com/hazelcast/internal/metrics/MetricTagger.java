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

import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;

/**
 * Interface to be used for tagging a metric.
 */
public interface MetricTagger {

    /**
     * Returns a new MetricTagger instance with the given general purpose tag added.
     *
     * @param tag   The name of the tag
     * @param value The value of the tag
     */
    MetricTagger withTag(String tag, String value);

    /**
     * Returns a new MetricTagger instance with the given identifier tag
     * added. The identifier tag will be used by the {@link MetricsRegistryImpl}
     * in the key of the tag as a discriminator if the metric built with
     * this MetricTagger is a static metric.
     *
     * @param tag   The name of the tag
     * @param value The value of the tag
     * @see #metricId()
     */
    MetricTagger withIdTag(String tag, String value);

    /**
     * Returns a new MetricTagger instance with the given metric name tag
     * added.
     *
     * @param value The value of the tag
     */
    MetricTagger withMetricTag(String value);

    /**
     * Returns the name for the metric. It is used by the {@link MetricsCollector}s
     * and {@link MetricsPublisher}s. The returned format is like
     * {@code [name=myMap,unit=count,metric=map.entryCount]}.
     * <p>
     * The value returned by this method is immutable.
     */
    String metricName();

    /**
     * Returns the id for the metric. Used in {@link MetricsRegistryImpl}
     * as the key of the static metric built with this MetricTagger. The
     * returned format is {@code prefix[idTagValue].metricName}. Non-ID tags
     * and ID tag name are ignored.
     * <p>
     * The purpose of the metricId is to make creation of {@link Gauge}s easier
     * and less error prone. With the {@link #metricName()}, changing the
     * order of the tags or adding/removing tags breaks existing gauges.
     * <p>
     * The value returned by this method is immutable.
     */
    String metricId();
}
