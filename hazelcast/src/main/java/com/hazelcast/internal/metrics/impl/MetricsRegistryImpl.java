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

import com.hazelcast.internal.metrics.DoubleGauge;
import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricTagger;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeFunction;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.StaticMetricsProvider;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.internal.util.ConcurrentReferenceHashMap;
import com.hazelcast.internal.util.concurrent.ThreadFactoryImpl;
import com.hazelcast.logging.ILogger;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.ConcurrentReferenceHashMap.Option.IDENTITY_COMPARISONS;
import static com.hazelcast.internal.util.ConcurrentReferenceHashMap.ReferenceType.STRONG;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.ThreadUtil.createThreadPoolName;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableSet;
import static java.util.EnumSet.of;

/**
 * The {@link MetricsRegistry} implementation.
 */
public class MetricsRegistryImpl implements MetricsRegistry {

    final ILogger logger;
    private final ProbeLevel minimumLevel;

    private final ScheduledExecutorService scheduler;
    private final ConcurrentMap<String, ProbeInstance> probeInstances = new ConcurrentHashMap<>();

    // use ConcurrentReferenceHashMap to allow unreferenced Class instances to be garbage collected
    private final ConcurrentMap<Class<?>, SourceMetadata> metadataMap
            = new ConcurrentReferenceHashMap<>();

    private final ConcurrentMap<DynamicMetricsProvider, Boolean> metricSourceMap
            = new ConcurrentReferenceHashMap<>(STRONG, STRONG, of(IDENTITY_COMPARISONS));

    /**
     * Creates a MetricsRegistryImpl instance.
     *
     * @param logger       the ILogger used
     * @param minimumLevel the minimum ProbeLevel. If a probe is registered with a ProbeLevel lower than the minimum ProbeLevel,
     *                     then the registration is skipped.
     * @throws NullPointerException if logger or minimumLevel is null
     */
    public MetricsRegistryImpl(ILogger logger, ProbeLevel minimumLevel) {
        this("default", logger, minimumLevel);
    }

    /**
     * Creates a MetricsRegistryImpl instance.
     *
     * @param name Name of the registry
     * @param logger       the ILogger used
     * @param minimumLevel the minimum ProbeLevel. If a probe is registered with a ProbeLevel lower than the minimum ProbeLevel,
     *                     then the registration is skipped.
     * @throws NullPointerException if logger or minimumLevel is null
     */
    public MetricsRegistryImpl(String name, ILogger logger, ProbeLevel minimumLevel) {
        this.logger = checkNotNull(logger, "logger can't be null");
        this.minimumLevel = checkNotNull(minimumLevel, "minimumLevel can't be null");
        this.scheduler = new ScheduledThreadPoolExecutor(2,
                new ThreadFactoryImpl(createThreadPoolName(name, "MetricsRegistry")));

        if (logger.isFinestEnabled()) {
            logger.finest("MetricsRegistry minimumLevel:" + minimumLevel);
        }
    }

    @Override
    public ProbeLevel minimumLevel() {
        return minimumLevel;
    }

    @Override
    public Set<String> getNames() {
        return unmodifiableSet(probeInstances.values().stream()
                                             .map(probeInstance -> probeInstance.name)
                                             .collect(Collectors.toSet()));
    }

    /**
     * Loads the {@link SourceMetadata}.
     *
     * @param clazz the Class to be analyzed.
     * @return the loaded SourceMetadata.
     */
    SourceMetadata loadSourceMetadata(Class<?> clazz) {
        SourceMetadata metadata = metadataMap.get(clazz);
        if (metadata == null) {
            metadata = new SourceMetadata(clazz);
            SourceMetadata found = metadataMap.putIfAbsent(clazz, metadata);
            metadata = found == null ? metadata : found;
        }

        return metadata;
    }

    @Override
    public <S> void registerStaticMetrics(S source, String namePrefix) {
        checkNotNull(source, "source can't be null");
        checkNotNull(namePrefix, "namePrefix can't be null");

        newMetricTagger(namePrefix).registerStaticMetrics(source);
    }

    @Override
    public void registerDynamicMetricsProvider(DynamicMetricsProvider metricsProvider) {
        metricSourceMap.put(metricsProvider, TRUE);
    }

    @Override
    public void deregisterDynamicMetricsProvider(DynamicMetricsProvider metricsProvider) {
        metricSourceMap.remove(metricsProvider);
    }

    @Override
    public <S> void registerStaticProbe(S source, String name, ProbeLevel level, LongProbeFunction<S> function) {
        checkNotNull(source, "source can't be null");
        checkNotNull(name, "name can't be null");
        checkNotNull(function, "function can't be null");
        checkNotNull(level, "level can't be null");

        registerInternal(source, newMetricTagger().withMetricTag(name), level, function);
    }

    @Override
    public <S> void registerStaticProbe(S source, String name, ProbeLevel level, DoubleProbeFunction<S> function) {
        checkNotNull(source, "source can't be null");
        checkNotNull(name, "name can't be null");
        checkNotNull(function, "function can't be null");
        checkNotNull(level, "level can't be null");

        MetricTagger metricTagger = newMetricTagger();
        registerInternal(source, metricTagger.withMetricTag(name), level, function);
    }

    ProbeInstance getProbeInstance(String name) {
        checkNotNull(name, "name can't be null");

        return probeInstances.get(name);
    }

    <S> void registerInternal(S source, MetricTagger tagger, ProbeLevel probeLevel, ProbeFunction function) {
        if (!probeLevel.isEnabled(minimumLevel)) {
            return;
        }

        String metricId = tagger.metricId();
        String metricName = tagger.metricName();

        ProbeInstance probeInstance = probeInstances
                .computeIfAbsent(metricId, k -> new ProbeInstance<>(metricName, source, function));

        if (probeInstance.source == source && probeInstance.function == function) {
            if (logger.isFinestEnabled()) {
                logger.finest("Registered probeInstance " + metricName);
            }
        } else {
            logOverwrite(probeInstance);
            probeInstance.source = source;
            probeInstance.function = function;
        }
    }

    private void logOverwrite(ProbeInstance probeInstance) {
        if (probeInstance.function != null || probeInstance.source != null) {
            logger.warning(format("Overwriting existing probe '%s'", probeInstance.name));
        }
    }

    @Override
    public LongGaugeImpl newLongGauge(String name) {
        checkNotNull(name, "name can't be null");

        return new LongGaugeImpl(this, name);
    }

    @Override
    public DoubleGauge newDoubleGauge(String name) {
        checkNotNull(name, "name can't be null");

        return new DoubleGaugeImpl(this, name);
    }

    @Override
    public void collect(MetricsCollector collector) {
        checkNotNull(collector, "collector can't be null");

        MetricsCollectionCycle collectionCycle = new MetricsCollectionCycle(this::newMetricTagger, this::loadSourceMetadata,
                collector, minimumLevel);

        collectionCycle.collectStaticMetrics(probeInstances.values());
        collectionCycle.collectDynamicMetrics(metricSourceMap.keySet());
    }

    @Override
    public void provideMetrics(Object... providers) {
        for (Object provider : providers) {
            if (provider instanceof StaticMetricsProvider) {
                ((StaticMetricsProvider) provider).provideStaticMetrics(this);
            }
        }
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable publisher, long period, TimeUnit timeUnit, ProbeLevel probeLevel) {
        if (!probeLevel.isEnabled(minimumLevel)) {
            return null;
        }
        return scheduler.scheduleAtFixedRate(publisher, 0, period, timeUnit);
    }

    public void shutdown() {
        // we want to immediately terminate; we don't want to wait till pending tasks have completed.
        scheduler.shutdownNow();
    }

    @Override
    public MetricTagger newMetricTagger() {
        return new MetricTaggerImpl(this, null);
    }

    @Override
    public MetricTagger newMetricTagger(String namePrefix) {
        return new MetricTaggerImpl(this, namePrefix);
    }
}
