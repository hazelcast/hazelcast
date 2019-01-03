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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.ProbeBuilder;
import com.hazelcast.internal.metrics.ProbeFunction;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;

import static com.hazelcast.internal.metrics.MetricsUtil.containsSpecialCharacters;
import static com.hazelcast.internal.metrics.MetricsUtil.escapeMetricNamePart;

public class ProbeBuilderImpl implements ProbeBuilder {

    private final MetricsRegistryImpl metricsRegistry;
    private final String keyPrefix;

    ProbeBuilderImpl(MetricsRegistryImpl metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
        this.keyPrefix = "[";
    }

    private ProbeBuilderImpl(MetricsRegistryImpl metricsRegistry, String keyPrefix) {
        this.metricsRegistry = metricsRegistry;
        this.keyPrefix = keyPrefix;
    }

    @Override
    @CheckReturnValue
    public ProbeBuilderImpl withTag(String tag, String value) {
        assert containsSpecialCharacters(tag) : "tag contains special characters";
        return new ProbeBuilderImpl(
                metricsRegistry, keyPrefix
                        + (keyPrefix.length() == 1 ? "" : ",")
                        + tag + '=' + escapeMetricNamePart(value));
    }

    @Override
    public String metricName() {
        return keyPrefix + ']';
    }

    @Override
    public <S> void register(
            @Nonnull S source,
            @Nonnull String metricName,
            @Nonnull ProbeLevel level,
            @Nonnull ProbeUnit unit,
            @Nonnull DoubleProbeFunction<S> probe
    ) {
        String name = this
                .withTag("unit", unit.name().toLowerCase())
                .withTag("metric", metricName)
                .metricName();
        metricsRegistry.register(source, name, level, probe);
    }

    @Override
    public <S> void register(
            @Nonnull S source,
            @Nonnull String metricName,
            @Nonnull ProbeLevel level,
            @Nonnull ProbeUnit unit,
            @Nonnull LongProbeFunction<S> probe
    ) {
        String name = this
                .withTag("unit", unit.name().toLowerCase())
                .withTag("metric", metricName)
                .metricName();
        metricsRegistry.register(source, name, level, probe);
    }

    <S> void register(S source, String metricName, ProbeLevel level, ProbeFunction probe) {
        metricsRegistry.registerInternal(source, withTag("metric", metricName).metricName(), level, probe);
    }

    @Override
    public <S> void scanAndRegister(S source) {
        SourceMetadata metadata = metricsRegistry.loadSourceMetadata(source.getClass());
        for (FieldProbe field : metadata.fields()) {
            field.register(this, source);
        }

        for (MethodProbe method : metadata.methods()) {
            method.register(this, source);
        }
    }
}
