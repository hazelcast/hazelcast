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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeFunction;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

import static com.hazelcast.internal.metrics.impl.MetricsUtil.adjustExclusionsWithLevel;
import static com.hazelcast.internal.metrics.impl.MetricsUtil.extractExcludedTargets;

/**
 * Class representing a metrics collection cycle. It collects both static
 * and dynamic metrics in each cycle.
 *
 * @see MetricsRegistry#collect(MetricsCollector)
 */
class MetricsCollectionCycle {
    private static final MetricValueCatcher NOOP_CATCHER = new NoOpMetricValueCatcher();

    private final PoolingMetricDescriptorSupplier descriptorSupplier;
    private final Function<Class, SourceMetadata> lookupMetadataFn;
    private final Function<MetricDescriptor, MetricValueCatcher> lookupMetricValueCatcherFn;
    private final MetricsCollector metricsCollector;
    private final ProbeLevel minimumLevel;
    private final MetricsContext metricsContext = new MetricsContext();
    private final long collectionId = System.nanoTime();
    private final ILogger logger = Logger.getLogger(MetricsCollectionCycle.class);

    MetricsCollectionCycle(Function<Class, SourceMetadata> lookupMetadataFn,
                           Function<MetricDescriptor, MetricValueCatcher> lookupMetricValueCatcherFn,
                           MetricsCollector metricsCollector,
                           ProbeLevel minimumLevel, MetricDescriptorReusableData metricDescriptorReusableData) {
        this.lookupMetadataFn = lookupMetadataFn;
        this.lookupMetricValueCatcherFn = lookupMetricValueCatcherFn;
        this.metricsCollector = metricsCollector;
        this.minimumLevel = minimumLevel;
        if (metricDescriptorReusableData == null) {
            this.descriptorSupplier = new PoolingMetricDescriptorSupplier();
        } else {
            this.descriptorSupplier = new PoolingMetricDescriptorSupplier(metricDescriptorReusableData);
        }
    }

    void collectStaticMetrics(Map<MetricDescriptorImpl.LookupView, ProbeInstance> probeInstanceEntries) {
        for (Map.Entry<MetricDescriptorImpl.LookupView, ProbeInstance> entry : probeInstanceEntries.entrySet()) {
            MetricDescriptorImpl.LookupView lookupView = entry.getKey();
            ProbeInstance probeInstance = entry.getValue();
            ProbeFunction function = probeInstance.function;

            lookupMetricValueCatcher(lookupView.descriptor()).catchMetricValue(collectionId, probeInstance, function);

            if (function instanceof LongProbeFunction) {
                collectLong(probeInstance.source, probeInstance.descriptor, (LongProbeFunction) function);
            } else if (function instanceof DoubleProbeFunction) {
                collectDouble(probeInstance.source, probeInstance.descriptor, (DoubleProbeFunction) function);
            } else {
                throw new IllegalStateException("Unhandled ProbeFunction encountered: " + function.getClass().getName());
            }
        }
    }

    void collectDynamicMetrics(Collection<DynamicMetricsProvider> metricsSources) {
        for (DynamicMetricsProvider metricsSource : metricsSources) {
            try {
                metricsSource.provideDynamicMetrics(descriptorSupplier.get(), metricsContext);
            } catch (Throwable t) {
                logger.warning("Collecting metrics from source " + metricsSource.getClass().getName() + " failed", t);
                assert false : "Collecting metrics from source " + metricsSource.getClass().getName() + " failed";
            }
        }
    }

    void notifyAllGauges(Collection<AbstractGauge> gauges) {
        for (AbstractGauge gauge : gauges) {
            gauge.onCollectionCompleted(collectionId);
        }
    }

    private MetricValueCatcher lookupMetricValueCatcher(MetricDescriptor descriptor) {
        MetricValueCatcher catcher = lookupMetricValueCatcherFn.apply(descriptor);
        return catcher != null ? catcher : NOOP_CATCHER;
    }

    private void extractAndCollectDynamicMetrics(MetricDescriptor descriptor, Object source) {
        SourceMetadata metadata = lookupMetadataFn.apply(source.getClass());

        for (MethodProbe methodProbe : metadata.methods()) {
            if (methodProbe.probe.level().isEnabled(minimumLevel)) {
                MetricDescriptor descriptorCopy = descriptor
                        .copy()
                        .withUnit(methodProbe.probe.unit())
                        .withMetric(methodProbe.getProbeName())
                        .withExcludedTargets(extractExcludedTargets(methodProbe, minimumLevel));

                lookupMetricValueCatcher(descriptorCopy).catchMetricValue(collectionId, source, methodProbe);
                collect(descriptorCopy, source, methodProbe);
            }
        }

        for (FieldProbe fieldProbe : metadata.fields()) {
            if (fieldProbe.probe.level().isEnabled(minimumLevel)) {
                MetricDescriptor descriptorCopy = descriptor
                        .copy()
                        .withUnit(fieldProbe.probe.unit())
                        .withMetric(fieldProbe.getProbeName())
                        .withExcludedTargets(extractExcludedTargets(fieldProbe, minimumLevel));

                lookupMetricValueCatcher(descriptorCopy).catchMetricValue(collectionId, source, fieldProbe);
                collect(descriptorCopy, source, fieldProbe);
            }
        }
    }

    private void collect(MetricDescriptor descriptor, Object source, ProbeFunction function) {
        if (function == null || source == null) {
            metricsCollector.collectNoValue(descriptor);
            return;
        }

        if (function instanceof LongProbeFunction) {
            LongProbeFunction longFunction = (LongProbeFunction) function;
            collectLong(source, descriptor, longFunction);
        } else {
            DoubleProbeFunction doubleFunction = (DoubleProbeFunction) function;
            collectDouble(source, descriptor, doubleFunction);
        }

        if (descriptor instanceof MetricDescriptorImpl) {
            descriptorSupplier.recycle((MetricDescriptorImpl) descriptor);
        }
    }

    private void collectDouble(Object source, MetricDescriptor descriptor, DoubleProbeFunction function) {
        try {
            double value = function.get(source);
            metricsCollector.collectDouble(descriptor, value);
        } catch (Exception ex) {
            metricsCollector.collectException(descriptor, ex);
        }
    }

    private void collectLong(Object source, MetricDescriptor descriptor, LongProbeFunction function) {
        try {
            long value = function.get(source);
            metricsCollector.collectLong(descriptor, value);
        } catch (Exception ex) {
            metricsCollector.collectException(descriptor, ex);
        }
    }

    public MetricDescriptorReusableData cleanUp() {
        return descriptorSupplier.close();
    }

    private class MetricsContext implements MetricsCollectionContext {
        @Override
        public void collect(MetricDescriptor descriptor, Object source) {
            extractAndCollectDynamicMetrics(descriptor, source);
        }

        @Override
        public void collect(MetricDescriptor descriptor, String name, ProbeLevel level, ProbeUnit unit, long value) {
            if (level.isEnabled(minimumLevel)) {
                MetricDescriptor descriptorCopy = descriptor
                        .copy()
                        .withUnit(unit)
                        .withMetric(name);
                adjustExclusionsWithLevel(descriptorCopy, level, minimumLevel);

                lookupMetricValueCatcher(descriptorCopy).catchMetricValue(collectionId, value);
                metricsCollector.collectLong(descriptorCopy, value);
            }
        }

        @Override
        public void collect(MetricDescriptor descriptor, String name, ProbeLevel level, ProbeUnit unit, double value) {
            if (level.isEnabled(minimumLevel)) {
                MetricDescriptor descriptorCopy = descriptor
                        .copy()
                        .withUnit(unit)
                        .withMetric(name);
                adjustExclusionsWithLevel(descriptorCopy, level, minimumLevel);

                lookupMetricValueCatcher(descriptorCopy).catchMetricValue(collectionId, value);
                metricsCollector.collectDouble(descriptorCopy, value);
            }
        }

        @Override
        public void collect(MetricDescriptor descriptor, long value) {
            lookupMetricValueCatcher(descriptor).catchMetricValue(collectionId, value);
            metricsCollector.collectLong(descriptor, value);
        }

        @Override
        public void collect(MetricDescriptor descriptor, double value) {
            lookupMetricValueCatcher(descriptor).catchMetricValue(collectionId, value);
            metricsCollector.collectDouble(descriptor, value);
        }
    }

    private static final class NoOpMetricValueCatcher implements MetricValueCatcher {

        @Override
        public void catchMetricValue(long collectionId, Object source, ProbeFunction function) {
            // noop
        }

        @Override
        public void catchMetricValue(long collectionId, long value) {
            // noop
        }

        @Override
        public void catchMetricValue(long collectionId, double value) {
            // noop
        }
    }
}

