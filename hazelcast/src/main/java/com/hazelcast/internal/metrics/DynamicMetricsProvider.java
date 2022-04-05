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

package com.hazelcast.internal.metrics;

/**
 * Interface to be implemented by the classes that expose metrics in a
 * dynamic fashion. The {@link #provideDynamicMetrics(MetricDescriptor, MetricsCollectionContext)}
 * method is called in every metric collection cycle. For more on dynamic
 * metrics, please see the documentation of {@link MetricsRegistry}.
 * <p/>
 * Registering instances of this interface should be done by calling
 * {@link MetricsRegistry#registerDynamicMetricsProvider(DynamicMetricsProvider)}
 */
public interface DynamicMetricsProvider {

    /**
     * Metrics collection callback that is called in every metric collection
     * cycle. The collected metrics should be passed to the
     * {@link MetricsCollectionContext} collection methods.
     * <p>
     * Note that {@code descriptor} is a blank descriptor and may be
     * recycled after collecting a metric. That means the following two
     * things:
     * <ul>
     *     <li>Reference must not be kept to this descriptor
     *     <li>If multiple calls are made to the {@code context} collection
     *     methods, {@link MetricDescriptor#copy()} should be used
     *     to get a new instance for every call, with the same content.
     * </ul>
     *
     * @param descriptor Blank descriptor for describing the collected
     *                   metric(s)
     * @param context    The context used to collect the metrics
     */
    void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context);
}
