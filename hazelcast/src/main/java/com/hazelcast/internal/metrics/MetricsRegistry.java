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

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The MetricsRegistry is responsible for recording all kinds of Hazelcast/JVM specific information to
 * help out with all kinds of issues.
 *
 * Each HazelcastInstance has its own MetricsRegistry.
 *
 * A MetricsRegistry can contain many {@link Metric} instances. Each metric has an input and each metric can be
 * identified using a name; a String. This name can be any string, the general structure is something like:
 * <ol>
 * <li>proxy.count</li>
 * <li>operation.completed.count</li>
 * <li>operation.partition[14].count</li>
 * </ol>
 * For the time being there the MetricsRegistry doesn't require any syntax for the name content; so any String is fine.
 *
 * <h1>Duplicate Registrations</h1>
 * The MetricsRegistry is lenient regarding duplicate registrations of metrics. If a metric is created and
 * an input is set, and then a new registration for the same Metric is done, the old input/source is overwritten.
 * The reason to be lenient is that the MetricRegistry should not throw exceptions. Of course, there will be a log
 * warning.
 *
 * <h1>Performance</h1>
 * The MetricRegistry is designed for low overhead metrics. Once a metric is registered, there is no overhead
 * for the provider of the Metric data. For example, the provider could have a volatile long field and increment
 * this using a lazy-set. As long as the MetricRegistry can frequently read out this field, the MetricRegistry
 * is perfectly happy with such low overhead inputs. It is up to the provider of the metric input to determine
 * how much overhead is required.
 */
public interface MetricsRegistry {

    /**
     * Scans the source object for any fields/methods that have been given a name prefix with {@link Probe}
     * annotation, and registering these fields/methods as metrics.
     *
     * If metrics with the same name already exist, their source/inputs will be updated. Multiple registrations
     * of the same object are ignored.
     *
     * If an object has no @Gauge annotations, the call is ignored.
     *
     * @param source     the source object to scan.
     * @param namePrefix search the source object for fields/methods that have this name prefix.
     * @throws NullPointerException     if namePrefix or source is null.
     * @throws IllegalArgumentException if the source contains Gauge annotation on a field/method of unsupported type.
     */
    <S> void scanAndRegister(S source, String namePrefix);

    /**
     * Registers a probe.
     *
     * If a Metric with the given name already has an input, that input will be overwritten.
     *
     * @param source the source object.
     * @param name   the name of the metric.
     * @param input  the input for the metric.
     * @throws NullPointerException if source, name or input is null.
     */
    <S> void register(S source, String name, LongProbe<S> input);

    /**
     * Registers a probe.
     *
     * If a Metric with the given name already has an input, that input will be overwritten.
     *
     * @param source the source object.
     * @param name   the name of the metric.
     * @param input  the input for the metric.
     * @throws NullPointerException if name or input is null.
     */
    <S> void register(S source, String name, DoubleProbe<S> input);


    /**
     * Deregisters and scans object with probe annotations. All metrics that were for this given source object are removed.
     *
     * If the object already is deregistered, the call is ignored.
     *
     * If the object was never registered, the call is ignored.
     *
     * @param source the object to deregister.
     * @throws NullPointerException if source is null.
     */
    <S> void deregister(S source);

    /**
     * Schedules a publisher to be periodically executed.
     *
     * Probably this method will be removed in the future, but we need a mechanism for complex gauges that require some
     * calculation to provide their values.
     *
     * @param publisher the published task that needs to be periodically executed.
     * @param period    the time between executions.
     * @param timeUnit  the timeunit for period.
     * @throws NullPointerException if publisher or timeUnit is null.
     */
    void scheduleAtFixedRate(Runnable publisher, long period, TimeUnit timeUnit);

    /**
     * Gets the Gauge for a given name.
     *
     * If no gauge exists for the name, it will be created but no input is set. The reason to do so is
     * that you don't want to depend on the order of registration. For example, you want to read out operations.count
     * gauge, but the OperationService has not started yet and the metric is not yet available.
     *
     * @param name the name of the gauge to get.
     * @return the Gauge. Multiple calls with the same name, return the same Metric instance.
     * @throws NullPointerException if name is null.
     */
    Gauge getGauge(String name);

    /**
     * Gets a set of all current metric names.
     *
     * @return set of all current metric names.
     */
    Set<String> getNames();

    /**
     * Returns the modCount. Every time a Metric is added or removed, the modCount is increased.
     *
     * Returned modCount will always be equal or larger than 0.
     *
     * @return the modCount: the number of times Metrics are added or removed.
     */
    int modCount();
}
