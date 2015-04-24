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

import com.hazelcast.internal.metrics.DoubleGauge;
import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeFunction;
import com.hazelcast.internal.metrics.metricsets.ClassLoadingMetricSet;
import com.hazelcast.internal.metrics.metricsets.GarbageCollectionMetricSet;
import com.hazelcast.internal.metrics.metricsets.OperatingSystemMetricsSet;
import com.hazelcast.internal.metrics.metricsets.RuntimeMetricSet;
import com.hazelcast.internal.metrics.metricsets.ThreadMetricSet;
import com.hazelcast.internal.metrics.renderers.ProbeRenderer;
import com.hazelcast.logging.ILogger;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * The {@link MetricsRegistry} implementation.
 */
public class MetricsRegistryImpl implements MetricsRegistry {

    final ILogger logger;

    private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(2);
    private final AtomicInteger modCount = new AtomicInteger();
    private final ConcurrentMap<String, ProbeInstance> probeInstances = new ConcurrentHashMap<String, ProbeInstance>();
    private final ConcurrentMap<Class<?>, SourceMetadata> metadataMap
            = new ConcurrentHashMap<Class<?>, SourceMetadata>();
    private final LockStripe lockStripe = new LockStripe();

    private AtomicReference<SortedProbesInstances> sortedProbeInstance = new AtomicReference<SortedProbesInstances>(
            new SortedProbesInstances()
    );

    /**
     * Creates a MetricsRegistryImpl instance.
     *
     * Automatically registers the com.hazelcast.internal.metrics.metricpacks.
     *
     * @param logger the ILogger used
     * @throws NullPointerException if logger is null
     */
    public MetricsRegistryImpl(ILogger logger) {
        this.logger = checkNotNull(logger, "logger can't be null");

        RuntimeMetricSet.register(this);
        GarbageCollectionMetricSet.register(this);
        OperatingSystemMetricsSet.register(this);
        ThreadMetricSet.register(this);
        ClassLoadingMetricSet.register(this);
    }

    int modCount() {
        return modCount.get();
    }

    @Override
    public Set<String> getNames() {
        Set<String> names = new HashSet<String>(probeInstances.keySet());
        return Collections.unmodifiableSet(names);
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
    public <S> void scanAndRegister(S source, String namePrefix) {
        checkNotNull(source, "source can't be null");
        checkNotNull(namePrefix, "namePrefix can't be null");

        SourceMetadata metadata = loadSourceMetadata(source.getClass());
        metadata.register(this, source, namePrefix);
    }

    @Override
    public <S> void register(S source, String name, LongProbeFunction<S> function) {
        checkNotNull(source, "source can't be null");
        checkNotNull(name, "name can't be null");
        checkNotNull(function, "function can't be null");

        registerInternal(source, name, function);
    }

    @Override
    public <S> void register(S source, String name, DoubleProbeFunction<S> function) {
        checkNotNull(source, "source can't be null");
        checkNotNull(name, "name can't be null");
        checkNotNull(function, "function can't be null");

        registerInternal(source, name, function);
    }

    public ProbeInstance getProbeInstance(String name) {
        checkNotNull(name, "name can't be null");

        return probeInstances.get(name);
    }

    <S> void registerInternal(S source, String name, ProbeFunction function) {
        synchronized (lockStripe.getLock(source)) {
            ProbeInstance probeInstance = probeInstances.get(name);
            if (probeInstance == null) {
                probeInstance = new ProbeInstance<S>(name, source, function);
                probeInstances.put(name, probeInstance);
            } else {
                logOverwrite(probeInstance);
            }

            if (logger.isFinestEnabled()) {
                logger.finest("Registered probeInstance " + name);
            }

            probeInstance.source = source;
            probeInstance.function = function;
        }
        modCount.incrementAndGet();
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
    public <S> void deregister(S source) {
        checkNotNull(source, "source can't be null");

        boolean changed = false;
        for (Map.Entry<String, ProbeInstance> entry : probeInstances.entrySet()) {
            ProbeInstance probeInstance = entry.getValue();

            if (probeInstance.source != source) {
                continue;
            }

            String name = entry.getKey();

            boolean destroyed = false;
            synchronized (lockStripe.getLock(source)) {
                if (probeInstance.source == source) {
                    changed = true;
                    probeInstances.remove(name);
                    probeInstance.source = null;
                    probeInstance.function = null;
                    destroyed = true;
                }
            }

            if (destroyed && logger.isFinestEnabled()) {
                logger.finest("Destroying probeInstance " + name);
            }
        }

        if (changed) {
            modCount.incrementAndGet();
        }
    }

    @Override
    public void render(ProbeRenderer renderer) {
        checkNotNull(renderer, "renderer can't be null");

        renderer.start();
        for (ProbeInstance probeInstance : getSortedProbeInstances()) {
            render(renderer, probeInstance);
        }
        renderer.finish();
    }

    /**
     * Returns the SortedProbesInstances. This method is eventually consistent; so eventually it will return
     * a SortedProbesInstances where the content exactly matches the mod-count. It can be that this method
     * returns a probe-instances in combination with a too old mod-count (in-consistent). This is not a
     * problem, since the next time this method is called, it will update the SortedProbeInstances again.
     *
     * If this method is being called while
     * probes are being added or removed, then it will return whatever is available.
     *
     * @return
     */
    SortedProbesInstances getSortedProbeInstances() {
        SortedProbesInstances sortedProbeInstances = this.sortedProbeInstance.get();
        int lastModCount = modCount.get();
        if (lastModCount == sortedProbeInstances.modCount) {
            return sortedProbeInstances;
        }

        SortedProbesInstances newSortedProbeInstances = new SortedProbesInstances(probeInstances.values(), lastModCount);
        this.sortedProbeInstance.compareAndSet(sortedProbeInstances, newSortedProbeInstances);
        return sortedProbeInstance.get();
    }

    private void render(ProbeRenderer renderer, ProbeInstance probeInstance) {
        ProbeFunction function = probeInstance.function;
        Object source = probeInstance.source;
        String name = probeInstance.name;

        if (function == null || source == null) {
            renderer.renderNoValue(name);
            return;
        }

        try {
            if (function instanceof LongProbeFunction) {
                LongProbeFunction longFunction = (LongProbeFunction) function;
                renderer.renderLong(name, longFunction.get(source));
            } else {
                DoubleProbeFunction doubleFunction = (DoubleProbeFunction) function;
                renderer.renderDouble(name, doubleFunction.get(source));
            }
        } catch (Exception e) {
            renderer.renderException(name, e);
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
