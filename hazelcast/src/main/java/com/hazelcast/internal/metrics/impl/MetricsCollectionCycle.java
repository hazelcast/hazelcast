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
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricTagger;
import com.hazelcast.internal.metrics.MetricTaggerSupplier;
import com.hazelcast.internal.metrics.MetricTarget;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeAware;
import com.hazelcast.internal.metrics.ProbeFunction;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;

import java.util.Collection;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.emptySet;

/**
 * Class representing a metrics collection cycle. It collects both static
 * and dynamic metrics in each cycle.
 *
 * @see MetricsRegistry#collect(MetricsCollector)
 */
class MetricsCollectionCycle {
    private final MetricTaggerSupplier taggerSupplier;
    private final Function<Class, SourceMetadata> lookupMetadataFunction;
    private final MetricsCollector metricsCollector;
    private final ProbeLevel minimumLevel;
    private final MetricsContext metricsContext = new MetricsContext();

    MetricsCollectionCycle(Function<Class, SourceMetadata> lookupMetadataFunction,
                           MetricsCollector metricsCollector, ProbeLevel minimumLevel) {
        this.taggerSupplier = new TaggerSupplier();
        this.lookupMetadataFunction = lookupMetadataFunction;
        this.metricsCollector = metricsCollector;
        this.minimumLevel = minimumLevel;
    }

    void collectStaticMetrics(Collection<ProbeInstance> probeInstances) {
        for (ProbeInstance probeInstance : probeInstances) {
            ProbeFunction function = probeInstance.function;
            if (function instanceof LongProbeFunction) {
                collectLong(probeInstance.source, probeInstance.name, (LongProbeFunction) function);
            } else if (function instanceof DoubleProbeFunction) {
                collectDouble(probeInstance.source, probeInstance.name, (DoubleProbeFunction) function);
            } else {
                throw new IllegalStateException("Unhandled ProbeFunction encountered: " + function.getClass().getName());
            }
        }
    }

    void collectDynamicMetrics(Collection<DynamicMetricsProvider> metricsSources) {
        for (DynamicMetricsProvider metricsSource : metricsSources) {
            metricsSource.provideDynamicMetrics(taggerSupplier, metricsContext);
        }
    }

    private void extractAndCollectDynamicMetrics(MetricTagger tagger, Object source) {
        SourceMetadata metadata = lookupMetadataFunction.apply(source.getClass());

        for (MethodProbe methodProbe : metadata.methods()) {
            if (methodProbe.probe.level().isEnabled(minimumLevel)) {
                String name = tagger
                        .withTag("unit", methodProbe.probe.unit().name().toLowerCase())
                        .withMetricTag(methodProbe.getProbeOrMethodName())
                        .metricName();
                collect(name, source, methodProbe);
            }
        }

        for (FieldProbe fieldProbe : metadata.fields()) {
            if (fieldProbe.probe.level().isEnabled(minimumLevel)) {
                String name = tagger
                        .withTag("unit", fieldProbe.probe.unit().name().toLowerCase())
                        .withMetricTag(fieldProbe.getProbeOrFieldName())
                        .metricName();
                collect(name, source, fieldProbe);
            }
        }
    }

    private void collect(String name, Object source, ProbeFunction function) {
        Set<MetricTarget> excludedTargets = getExcludedTargets(function);

        if (function == null || source == null) {
            metricsCollector.collectNoValue(name, excludedTargets);
            return;
        }

        if (function instanceof LongProbeFunction) {
            LongProbeFunction longFunction = (LongProbeFunction) function;
            collectLong(source, name, longFunction);
        } else {
            DoubleProbeFunction doubleFunction = (DoubleProbeFunction) function;
            collectDouble(source, name, doubleFunction);
        }
    }

    private void collectDouble(Object source, String name, DoubleProbeFunction function) {
        Set<MetricTarget> excludedTargets = getExcludedTargets(function);
        try {
            double value = function.get(source);
            metricsCollector.collectDouble(name, value, excludedTargets);
        } catch (Exception ex) {
            metricsCollector.collectException(name, ex, excludedTargets);
        }
    }

    private void collectLong(Object source, String name, LongProbeFunction function) {
        Set<MetricTarget> excludedTargets = getExcludedTargets(function);
        try {
            long value = function.get(source);
            metricsCollector.collectLong(name, value, excludedTargets);
        } catch (Exception ex) {
            metricsCollector.collectException(name, ex, excludedTargets);
        }
    }

    private Set<MetricTarget> getExcludedTargets(Object object) {
        if (object instanceof ProbeAware) {
            Probe probe = ((ProbeAware) object).getProbe();
            return MetricTarget.asSet(probe.excludedTargets());
        }

        return emptySet();
    }

    private class MetricsContext implements MetricsCollectionContext {

        @Override
        public void collect(MetricTagger metricTagger, Object source) {
            extractAndCollectDynamicMetrics(metricTagger, source);
        }

        @Override
        public void collect(MetricTagger tagger, String name, ProbeLevel level, ProbeUnit unit, long value) {
            if (level.isEnabled(minimumLevel)) {
                metricsCollector.collectLong(tagger.withTag("unit", unit.name().toLowerCase())
                                                   .withMetricTag(name)
                                                   .metricName(), value, emptySet());
            }
        }


        @Override
        public void collect(MetricTagger tagger, String name, ProbeLevel level, ProbeUnit unit, double value) {
            if (level.isEnabled(minimumLevel)) {
                metricsCollector.collectDouble(tagger.withTag("unit", unit.name().toLowerCase())
                                                     .withMetricTag(name)
                                                     .metricName(), value, emptySet());
            }
        }
    }

    private static class TaggerSupplier implements MetricTaggerSupplier {

        @Override
        public MetricTagger getMetricTagger() {
            return getMetricTagger(null);
        }

        @Override
        public MetricTagger getMetricTagger(String namespace) {
            return new MetricTaggerImpl(namespace);
        }
    }
}
