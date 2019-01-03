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

import javax.annotation.Nonnull;

/**
 * Immutable builder object to register Probes.
 */
public interface ProbeBuilder {

    /**
     * Returns a new ProbeBuilder instance with the given tag added.
     */
    ProbeBuilder withTag(String tag, String value);

    /**
     * Registers a single probe.
     * <p>
     * If a probe for the given name exists, it will be overwritten silently.
     *
     * @param source the object to pass to probeFn
     * @param metricName the value of "metric" tag
     * @param level the ProbeLevel
     * @param unit the unit
     * @param probeFn the probe function
     * @throws NullPointerException if any of the arguments is null
     */
    <S> void register(
            @Nonnull S source,
            @Nonnull String metricName,
            @Nonnull ProbeLevel level,
            @Nonnull ProbeUnit unit,
            @Nonnull DoubleProbeFunction<S> probeFn);

    /**
     * Registers a single probe.
     * <p>
     * If a probe for the given name exists, it will be overwritten silently.
     *
     * @param source the object to pass to probeFn
     * @param metricName the value of "metric" tag
     * @param level the ProbeLevel
     * @param unit the unit
     * @param probeFn the probe function
     * @throws NullPointerException if any of the arguments is null
     */
    <S> void register(
            @Nonnull S source,
            @Nonnull String metricName,
            @Nonnull ProbeLevel level,
            @Nonnull ProbeUnit unit,
            @Nonnull LongProbeFunction<S> probeFn);

    /**
     * Scans the source object for any fields/methods that have been annotated
     * with {@link Probe} annotation, and registers these fields/methods as
     * probe instances.
     * <p>
     * If a probe with the same name already exists, the probe is overwritten
     * silently.
     * <p>
     * If an object has no @Probe annotations, the call is ignored.
     *
     * @param source     the object to scan
     * @throws IllegalArgumentException if the source contains Probe annotation
     * on a field/method of unsupported type.
     */
    <S> void scanAndRegister(S source);

    /**
     * Returns the name for the metric.
     */
    String metricName();
}

