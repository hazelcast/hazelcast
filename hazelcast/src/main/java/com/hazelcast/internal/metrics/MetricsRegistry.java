/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.renderers.ProbeRenderer;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The MetricsRegistry is responsible for recording all kinds of Hazelcast/JVM specific information to
 * help out with issues like performance or stability problems.
 *
 * Each HazelcastInstance has its own MetricsRegistry instance.
 *
 * A MetricsRegistry can contain many {@link Probe} instances. A probe is registered under a certain name,
 * and can be read by creating a Gauge, see {@link #newLongGauge(String)}.
 *
 * This name can be any string, e.g.:
 * <ol>
 * <li>proxy.count</li>
 * <li>operation.completed.count</li>
 * <li>operation.partition[14].count</li>
 * </ol>
 * For the time being there the MetricsRegistry doesn't require any syntax for the name content; so any String is fine.
 *
 * <h1>Duplicate Registrations</h1>
 * The MetricsRegistry is lenient regarding duplicate registrations of probes. So if there is an existing probe for a
 * given name and a new probe with the same name is registered, the old probe is overwritten. The reason to be lenient
 * is that the MetricRegistry should not throw exception. Of course there will be a log warning.
 *
 * <h1>Performance</h1>
 * The MetricRegistry is designed for low overhead probes. So once a probe is registered, there is no overhead
 * for the provider of the probe data. The provider could have for example a volatile long field and increment
 * this using a lazy-set. As long as the MetricRegistry can frequently read out this field, the MetricRegistry
 * is perfectly happy with such low overhead probes. So it is up to the provider of the probe
 * how much overhead is required.
 */
public interface MetricsRegistry {

    /**
     * Creates a LongGauge for a given metric name.
     *
     * If no gauge exists for the name, it will be created but no probe is set. The reason to do so is that you don't want to
     * depend on the order of registration. Perhaps you want to read out e.g. operations.count gauge, but the OperationService
     * has not started yet and the metric is not yet available. Another cause is that perhaps a probe is not registered, but
     * the metric is created. For example when experimenting with a new implementation, e.g. a new OperationService
     * implementation, that doesn't provide the operation.count probe.
     *
     * Multiple calls with the same name, return different Gauge instances; so the Gauge instance is not cached. This is
     * done to prevent memory leaks.
     *
     * @param name the name of the metric.
     * @return the created LongGauge.
     * @throws NullPointerException if name is null.
     */
    LongGauge newLongGauge(String name);

    /**
     * Creates a DoubleProbe for a given metric name.
     *
     * @param name name of the metric
     * @return the create DoubleGauge
     * @throws NullPointerException if name is null.
     * @see #newLongGauge(String)
     */
    DoubleGauge newDoubleGauge(String name);

    /**
     * Gets a set of all current probe names.
     *
     * The returned set is immutable and is a snapshot of the names. So the reader gets a stable view on the names.
     *
     * @return set of all current names.
     */
    Set<String> getNames();

    /**
     * Scans the source object for any fields/methods that have been annotated with {@link Probe} annotation, and
     * registering these fields/methods as probes instances.
     *
     * If a probe is called, 'queueSize' and the namePrefix is 'operations, then the name of the probe-instance
     * is 'operations.queueSize'.
     *
     * If probes with the same name already exist, then the probes are replaced.
     *
     * If an object has no @Gauge annotations, the call is ignored.
     *
     * @param source     the object to scan.
     * @param namePrefix the name prefix.
     * @throws NullPointerException     if namePrefix or source is null.
     * @throws IllegalArgumentException if the source contains Gauge annotation on a field/method of unsupported type.
     */
    <S> void scanAndRegister(S source, String namePrefix);

    /**
     * Registers a probe.
     *
     * If a probe for the given name exists, it will be overwritten.
     *
     * @param name  the name of the probe.
     * @param probe the probe
     * @throws NullPointerException if source, name or probe is null.
     */
    <S> void register(S source, String name, LongProbeFunction<S> probe);

    /**
     * Registers a probe
     *
     * If a probe for the given name exists, it will be overwritten.
     *
     * @param name  the name of the probe
     * @param probe the probe
     * @throws NullPointerException if name or probe is null.
     */
    <S> void register(S source, String name, DoubleProbeFunction<S> probe);

    /**
     * Deregisters all probes for a given source object.
     *
     * If the object already is deregistered, the call is ignored.
     *
     * If the object was never registered, the call is ignored.
     *
     * @param source the object to deregister
     * @throws NullPointerException if source is null.
     */
    <S> void deregister(S source);

    /**
     * Schedules a publisher to be executed at a fixed rate.
     *
     * Probably this method will be removed in the future, but we need a mechanism for complex gauges that require some
     * calculation to provide their values.
     *
     * @param publisher the published task that needs to be executed
     * @param period    the time between executions
     * @param timeUnit  the timeunit for period
     * @throws NullPointerException if publisher or timeUnit is null.
     */
    void scheduleAtFixedRate(Runnable publisher, long period, TimeUnit timeUnit);

    /**
     * Renders the content of the MetricsRegistry.
     *
     * @param renderer the ProbeRenderer
     * @throws NullPointerException if renderer is null.
     */
    void render(ProbeRenderer renderer);
}
