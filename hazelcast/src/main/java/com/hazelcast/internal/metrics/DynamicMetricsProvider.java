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
 * Interface to be implemented by the classes that expose metrics in a
 * dynamic fashion. The {@link #provideDynamicMetrics(MetricTaggerSupplier, MetricsCollectionContext)}
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
     * {@link MetricsCollectionContext} passed in argument.
     *
     * @param context The context used to collect the metrics
     */
    void provideDynamicMetrics(MetricTaggerSupplier taggerSupplier, MetricsCollectionContext context);
}
