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

import com.hazelcast.internal.metrics.collectors.MetricsCollector;

import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The MetricsRegistry is a registry of various Hazelcast/JVM internal
 * information to help out with debugging, performance or stability issues.
 * Each HazelcastInstance has one local MetricsRegistry instance.
 * <p>
 * A MetricsRegistry can contain many {@link Probe} instances. A probe is
 * registered under a name, and can be read by creating a {@link Gauge}, see
 * {@link #newLongGauge(String)}.
 * <p>
 * The metrics registry doesn't interpret the name in any way, it is treated as
 * is. For example, {@code [tag1=foo,tag2=bar]} and {@code [tag2=bar,tag1=foo]}
 * will be treated as two different metrics even though they have same tags and
 * values. Clients making use of the metrics can use the tags. For backwards
 * compatibility, the name does not have to be enclosed in {@code []}.
 *
 * <h3>Duplicate Registrations</h3> The MetricsRegistry is lenient regarding
 * duplicate registrations of probes. So if there is an existing probe for a
 * given name and a new probe with the same name is registered, the old probe
 * is overwritten. The reason to be lenient is that the MetricRegistry should
 * not throw exception. Of course, there will be a log warning.
 *
 * <h3>Performance</h3> The MetricRegistry is designed for low overhead probes.
 * So once a probe is registered, there is no overhead for the provider of the
 * probe data. The provider could have for example a volatile long field and
 * increment this using a lazy-set. As long as the MetricRegistry can
 * frequently read out this field, the MetricRegistry is perfectly happy with
 * such low overhead probes. So it is up to the provider of the probe how much
 * overhead is required.
 *
 * <h3>Static and dynamic metrics</h3> The MetricRegistry collects metrics
 * from static and dynamic metrics sources, these metrics are referred to as
 * static and dynamic metrics.
 * <p/>
 * The static metrics are the ones that are registered once and cannot be
 * removed during the lifetime of the given Hazelcast instance. These are
 * typically system metrics either Hazelcast or JVM/OS ones.
 * <p/>
 * The dynamic metrics are collected dynamically during each collection cycle
 * via the {@link DynamicMetricsProvider} interface. Typical examples for the
 * dynamic metrics are the metrics exposed by the distributed data structures
 * that can be created and destroyed dynamically.
 * <p/>
 * The MetricsRegistry doesn't cache the dynamic metrics, therefore the dynamic
 * metrics don't increase the heap live set. In exchange, they may allocate
 * during the collection cycle. It is therefore the responsibility of the
 * dynamic metric sources to keep allocation low.
 */
public interface MetricsRegistry {

    /**
     * Returns the minimum ProbeLevel this MetricsRegistry is recording.
     */
    ProbeLevel minimumLevel();

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
     * Gets a set of all current probe names.
     *
     * The returned set is immutable and is a snapshot of the names. So the
     * reader gets a stable view on the names.
     *
     * @return set of all current names.
     */
    Set<String> getNames();

    /**
     * Scans the source object for any fields/methods that have been annotated
     * with {@link Probe} annotation, and registers these fields/methods as
     * static probe instances.
     * <p>
     * If a probe is called 'queueSize' and the namePrefix is 'operations',
     * then the name of the probe instance is 'operations.queueSize'.
     * <p>
     * If a probe with the same name already exists, then the probe is replaced.
     * <p>
     * If an object has no @Probe annotations, the call is ignored.
     *
     * @param source     the object to scan.
     * @param namePrefix the name prefix.
     * @throws NullPointerException     if namePrefix or source is null.
     * @throws IllegalArgumentException if the source contains a Probe
     *      annotation on a field/method of unsupported type.
     */
    <S> void registerStaticMetrics(S source, String namePrefix);

    /**
     * Scans the source object for any fields/methods that have been annotated
     * with {@link Probe} annotation, and registers these fields/methods as
     * static probe instances.
     * <p>
     * If a probe is called 'queueSize' and the namePrefix is 'operations',
     * then the name of the probe instance is 'operations.queueSize'.
     * <p>
     * If a probe with the same name already exists, then the probe is replaced.
     * <p>
     * If an object has no @Probe annotations, the call is ignored.
     *
     * @param descriptor the metric descriptor.
     * @param source the object to scan.
     * @throws NullPointerException     if namePrefix or source is null.
     * @throws IllegalArgumentException if the source contains a Probe
     *                                  annotation on a field/method of unsupported type.
     */
    <S> void registerStaticMetrics(MetricDescriptor descriptor, S source);

    /**
     * Registers dynamic metrics sources that collect metrics in each metrics
     * collection cycle.
     *
     * @param metricsProvider The object that provides dynamic metrics
     */
    void registerDynamicMetricsProvider(DynamicMetricsProvider metricsProvider);

    /**
     * Deregisters the given dynamic metrics provider. The metrics collection
     * cycles after this call will not call the given metrics provider until
     * it is registered again.
     *
     * @param metricsProvider The metrics provider to deregister
     */
    void deregisterDynamicMetricsProvider(DynamicMetricsProvider metricsProvider);

    /**
     * Registers a probe.
     *
     * If a probe for the given name exists, it will be overwritten.
     *
     * @param source   the object that the probe function to be used with
     * @param name     the name of the probe
     * @param level    the ProbeLevel
     * @param unit     the unit
     * @param function the probe function
     * @throws NullPointerException if source, name, level or probe is null.
     */
    <S> void registerStaticProbe(S source, MetricDescriptor descriptor, String name, ProbeLevel level, ProbeUnit unit,
                                 ProbeFunction function);

    /**
     * Registers a probe.
     *
     * If a probe for the given name exists, it will be overwritten.
     *
     * @param source   the object that the probe function to be used with
     * @param name     the name of the probe.
     * @param level    the ProbeLevel
     * @param function the probe
     * @throws NullPointerException if source, name, level or probe is null.
     */
    <S> void registerStaticProbe(S source, String name, ProbeLevel level, LongProbeFunction<S> function);

    /**
     * Registers a probe.
     *
     * If a probe for the given name exists, it will be overwritten.
     *
     * @param source the object that the probe function to be used with
     * @param name   the name of the probe.
     * @param level  the ProbeLevel
     * @param unit   the unit
     * @param probe  the probe
     * @throws NullPointerException if source, name, level or probe is null.
     */
    <S> void registerStaticProbe(S source, String name, ProbeLevel level, ProbeUnit unit, LongProbeFunction<S> probe);

    /**
     * Registers a probe.
     *
     * If a probe for the given name exists, it will be overwritten.
     *
     * @param source the object that the probe function to be used with
     * @param name   the name of the probe.
     * @param level  the ProbeLevel
     * @param unit   the unit
     * @param probe  the probe
     * @throws NullPointerException if source, name, level or probe is null.
     */
    <S> void registerStaticProbe(S source, MetricDescriptor descriptor, String name, ProbeLevel level, ProbeUnit unit,
                                 LongProbeFunction<S> probe);

    /**
     * Registers a probe.
     *
     * If a probe for the given name exists, it will be overwritten.
     *
     * @param source   the object that the probe function to be used with
     * @param name  the name of the probe
     * @param level the ProbeLevel
     * @param probe the probe
     * @throws NullPointerException if source, name, level or probe is null.
     */
    <S> void registerStaticProbe(S source, String name, ProbeLevel level, DoubleProbeFunction<S> probe);

    /**
     * Registers a probe.
     *
     * If a probe for the given name exists, it will be overwritten.
     *
     * @param source the object that the probe function to be used with
     * @param name   the name of the probe
     * @param level  the ProbeLevel
     * @param unit   the unit
     * @param probe  the probe
     * @throws NullPointerException if source, name, level or probe is null.
     */
    <S> void registerStaticProbe(S source, String name, ProbeLevel level, ProbeUnit unit, DoubleProbeFunction<S> probe);

    /**
     * Registers a probe.
     *
     * If a probe for the given name exists, it will be overwritten.
     *
     * @param source the object that the probe function to be used with
     * @param name   the name of the probe
     * @param level  the ProbeLevel
     * @param unit   the unit
     * @param probe  the probe
     * @throws NullPointerException if source, name, level or probe is null.
     */
    <S> void registerStaticProbe(S source, MetricDescriptor descriptor, String name, ProbeLevel level, ProbeUnit unit,
                                 DoubleProbeFunction<S> probe);

    /**
     * Schedules a publisher to be executed at a fixed rate.
     *
     * Probably this method will be removed in the future, but we need a mechanism
     * for complex gauges that require some calculation to provide their values.
     *
     * @param publisher the published task that needs to be executed
     * @param period    the time between executions
     * @param timeUnit  the time unit for period
     * @param probeLevel the ProbeLevel publisher it publishing on. This is needed to prevent scheduling
     *                   publishers if their probe level isn't sufficient.
     * @throws NullPointerException if publisher or timeUnit is null.
     * @return the ScheduledFuture that can be used to cancel the task, or null if nothing got scheduled.
     */
    ScheduledFuture<?> scheduleAtFixedRate(Runnable publisher, long period, TimeUnit timeUnit, ProbeLevel probeLevel);

    /**
     * Collects the content of the MetricsRegistry.
     *
     * @param collector the collector that consumes the metrics collected
     * @throws NullPointerException if collector is null.
     */
    void collect(MetricsCollector collector);

    /**
     * For each object that implements {@link StaticMetricsProvider} the
     * {@link StaticMetricsProvider#provideStaticMetrics(MetricsRegistry)} is called.
     *
     * @param providers the array of objects to initialize.
     */
    void provideMetrics(Object... providers);

    /**
     * Creates a new {@link MetricDescriptor}.
     */
    MetricDescriptor newMetricDescriptor();

}
