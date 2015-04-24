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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.DoubleProbe;
import com.hazelcast.internal.metrics.Gauge;
import com.hazelcast.internal.metrics.LongProbe;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.metricsets.ClassLoadingMetricSet;
import com.hazelcast.internal.metrics.metricsets.GarbageCollectionMetricSet;
import com.hazelcast.internal.metrics.metricsets.OperatingSystemMetricsSet;
import com.hazelcast.internal.metrics.metricsets.RuntimeMetricSet;
import com.hazelcast.internal.metrics.metricsets.ThreadMetricSet;
import com.hazelcast.logging.ILogger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * The {@link MetricsRegistry} implementation.
 */
public class MetricsRegistryImpl implements MetricsRegistry {

    private final ILogger logger;
    private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(2);
    private final AtomicInteger modCount = new AtomicInteger();
    private final ConcurrentMap<String, GaugeImpl> metrics = new ConcurrentHashMap<String, GaugeImpl>();
    private final ConcurrentMap<Class<?>, SourceMetadata> metadataMap
            = new ConcurrentHashMap<Class<?>, SourceMetadata>();

    /**
     * Creates a MetricsRegistryImpl instance.
     *
     * Automatically registers the com.hazelcast.internal.metrics.metricpacks.
     *
     * @param logger the ILogger used
     * @throws NullPointerException if logger is null
     */
    public MetricsRegistryImpl(ILogger logger) {
        this.logger = checkNotNull(logger, "Logger can't be null");

        RuntimeMetricSet.register(this);
        GarbageCollectionMetricSet.register(this);
        OperatingSystemMetricsSet.register(this);
        ThreadMetricSet.register(this);
        ClassLoadingMetricSet.register(this);
    }

    @Override
    public int modCount() {
        return modCount.get();
    }

    @Override
    public Set<String> getNames() {
        return metrics.keySet();
    }

    SourceMetadata getObjectMetadata(Class<?> clazz) {
        SourceMetadata metadata = metadataMap.get(clazz);
        if (metadata == null) {
            metadata = new SourceMetadata(clazz);
            SourceMetadata found = metadataMap.putIfAbsent(clazz, metadata);
            metadata = found == null ? metadata : found;
        }

        return metadata;
    }

    @Override
    public synchronized <S> void scanAndRegister(S source, String namePrefix) {
        checkNotNull(source, "source can't be null");
        checkNotNull(namePrefix, "namePrefix can't be null");

        SourceMetadata metadata = getObjectMetadata(source.getClass());
        metadata.register(this, source, namePrefix);
    }

    @Override
    public synchronized <S> void register(S source, String name, LongProbe<S> input) {
        checkNotNull(name, "source can't be null");
        checkNotNull(name, "name can't be null");
        checkNotNull(name, "input can't be null");

        registerInternal(source, name, input);
    }

    @Override
    public synchronized <S> void register(S source, String name, DoubleProbe<S> input) {
        checkNotNull(name, "source can't be null");
        checkNotNull(name, "name can't be null");
        checkNotNull(name, "input can't be null");

        registerInternal(source, name, input);
    }

    <S> void registerInternal(S source, String name, Object input) {
        GaugeImpl gauge = metrics.get(name);
        if (gauge == null) {
            gauge = new GaugeImpl<S>(name, logger);
            metrics.put(name, gauge);
        }

        logOverwrite(name, gauge);

        if (logger.isFinestEnabled()) {
            logger.finest("Registered gauge " + name);
        }

        gauge.source = source;
        gauge.input = input;

        modCount.incrementAndGet();
    }

    /**
     * We are going to check if a source or input already exist. If it exists, we are going the log a warning but we
     * are not going to fail. The MetricsRegistry should never fail unless the arguments don't make any sense of course.
     */
    private void logOverwrite(String name, GaugeImpl gauge) {
        // if an input already exists, we are just going to overwrite it.
        if (gauge.input != null) {
            logger.warning(format("Duplicate registration, a input for Metric '%s' already exists", name));
        }

        // if a source already exists, we are just going to overwrite it.
        if (gauge.source != null) {
            logger.warning(format("Duplicate registration, a input for Metric '%s' already exists", name));
        }
    }

    @Override
    public synchronized Gauge getGauge(String name) {
        checkNotNull(name, "name can't be null");

        GaugeImpl gauge = metrics.get(name);

        if (gauge == null) {
            gauge = new GaugeImpl(name, logger);
            metrics.put(name, gauge);
        }

        return gauge;
    }

    @Override
    public synchronized <S> void deregister(S source) {
        checkNotNull(source, "source can't be null");

        boolean changed = false;
        for (Map.Entry<String, GaugeImpl> entry : metrics.entrySet()) {
            GaugeImpl gauge = entry.getValue();
            if (gauge.source != source) {
                continue;
            }

            String name = entry.getKey();
            changed = true;
            metrics.remove(name);
            gauge.source = null;
            gauge.input = null;

            if (logger.isFinestEnabled()) {
                logger.finest("Destroying gauge " + name);
            }
        }

        if (changed) {
            modCount.incrementAndGet();
        }
    }

    @Override
    public void scheduleAtFixedRate(final Runnable publisher, long period, TimeUnit timeUnit) {
        scheduledExecutorService.scheduleAtFixedRate(publisher, 0, period, timeUnit);
    }

    public void shutdown() {
        scheduledExecutorService.shutdown();
    }
}
