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

package com.hazelcast.jet.core.metrics;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.impl.metrics.MetricsImpl;

/**
 * Utility class for obtaining handlers to user-defined metrics.
 * <p>
 * User-defined metrics are simple numeric values used to count or measure
 * things. A code submitted to Jet can use them to publish any custom run-time
 * values.
 *
 * @since Jet 4.0
 */
public final class Metrics {

    private Metrics() {
    }

    /**
     * Returns a non-thread-safe handler for manipulating the metric with the
     * specified name.
     * <p>
     * This method only works if called from a processor thread and the
     * returned handler is tied to the processor that is currently executing.
     * The handler is created on the first call, subsequent calls return the
     * cached instance and ignore the requested unit or thread safety. Until
     * the first call is made, the metric is not visible in JMX or using {@link
     * Job#getMetrics()}.
     */
    public static Metric metric(String name) {
        return MetricsImpl.metric(name, Unit.COUNT);
    }

    /**
     * Same as {@link #metric(String)}, but allows us to also specify the
     * measurement {@link Unit} of the metric.
     */
    public static Metric metric(String name, Unit unit) {
        return MetricsImpl.metric(name, unit);
    }

    /**
     * Returns a thread-safe handler for manipulating the metric with the
     * specified name and measurement unit.
     * <p>
     * You need the thread-safe method if you submit work to other threads and
     * want to manipulate the metric in those threads. For example:
     * <pre>
     * {@code
     *  p.readFrom(...)
     *   .mapUsingServiceAsync(
     *       nonSharedService(pctx -> 10L),
     *       (ctx, item) -> {
     *           // need to use thread-safe metric since it will be mutated from another thread
     *           Metric mapped = Metrics.threadSafeMetric("mapped", Unit.COUNT);
     *           return CompletableFuture.supplyAsync(
     *               () -> {
     *                   mapped.increment();
     *                   return item * ctx;
     *               }
     *           );
     *       }
     *   )
     * }</pre>
     *
     * <p>
     * This method only works if called from a processor thread and the
     * returned handler is tied to the processor that is currently executing.
     * The handler is created on the first call, subsequent calls return the
     * cached instance and ignore the requested unit or thread safety. Until
     * the first call is made, the metric is not visible in JMX or using {@link
     * Job#getMetrics()}.
     */
    public static Metric threadSafeMetric(String name) {
        return MetricsImpl.threadSafeMetric(name, Unit.COUNT);
    }

    /**
     * Same as {@link #threadSafeMetric(String)}, but allows us to also
     * specify the measurement {@link Unit} of the metric.
     */
    public static Metric threadSafeMetric(String name, Unit unit) {
        return MetricsImpl.threadSafeMetric(name, unit);
    }
}
