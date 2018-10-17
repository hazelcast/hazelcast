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

package com.hazelcast.internal.metrics;

import com.hazelcast.spi.annotation.PrivateApi;

/**
 * A service made accessible to core services so they have a chance to register
 * the "root objects" of the system.
 *
 * There is no need to unregister them later. Filtered
 * {@link #openContext(Class...)} can be used to restrict used
 * {@link MetricsSource}s.
 *
 * Metrics about things that change over time require a corresponding
 * {@link MetricsSource} (existing since the startup) that does probe the
 * available instances as they appear and stops doing that as they disappear.
 */
@PrivateApi
public interface MetricsRegistry {

    /**
     * Creates a {@link LongGauge} for a given metric name.
     *
     * If no gauge exists for the name, it will be created but no probe is set.
     * The reason to do so is that you don't want to depend on the order of
     * registration. Perhaps you want to read out e.g. operations.count gauge,
     * but the OperationService has not started yet and the metric is not yet
     * available. Another cause is that perhaps a probe is not registered, but
     * the metric is created. For example when experimenting with a new
     * implementation, e.g. a new OperationService implementation, that doesn't
     * provide the operation.count probe.
     *
     * Multiple calls with the same name return different Gauge instances; so
     * the Gauge instance is not cached. This is done to prevent memory leaks.
     *
     * @param name the name of the metric.
     * @return the created LongGauge.
     * @throws NullPointerException if name is null.
     */
    LongGauge newLongGauge(String name);

    /**
     * Creates a {@link DoubleGauge} for a given metric name.
     *
     * @param name name of the metric
     * @return the create DoubleGauge
     * @throws NullPointerException if name is null.
     * @see #newLongGauge(String)
     */
    DoubleGauge newDoubleGauge(String name);

    /**
     * Called once at startup, typically by a core service registering itself.
     *
     * @param root Typically implementation of a {@link MetricsSource} that
     *        "knows" how to make metrics known to a {@link CollectionCycle} in
     *        their appropriate context. Each sources is assumed to be unique per
     *        class. That means a second registration of a source is considered
     *        identical and therefore unnecessary if the source has the same type as
     *        an already registered source. Such a source registration is simply
     *        ignored. This is the most practical behavior as we usually do not want
     *        same source more then once.
     *
     *        If the source instance does not implement {@link MetricsSource} it is
     *        checked for {@link Probe} annotations and collected as if another
     *        source passed it to {@link CollectionCycle#collectAll(Object)}. If there
     *        also are no annotation the instance is ignored and has no effect.
     */
    void register(Object root);

    /**
     * Creates a new "private "context that should be kept by the caller to collect
     * the contents of this {@link MetricsRegistry}.
     *
     * The implementation will not support multi-threading as each thread should
     * create its own context instance.
     *
     * @param level the finest level that still should be collected
     * @return a new private "context". The context is updated when
     *         further roots are {@link #register(Object)}ed.
     */
    CollectionContext openContext(ProbeLevel level);

}
