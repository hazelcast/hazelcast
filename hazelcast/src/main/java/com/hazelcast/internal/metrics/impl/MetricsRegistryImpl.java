/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.DiscardableMetricsProvider;
import com.hazelcast.internal.metrics.DoubleGauge;
import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeBuilder;
import com.hazelcast.internal.metrics.ProbeFunction;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.renderers.ProbeRenderer;
import com.hazelcast.internal.util.concurrent.ThreadFactoryImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ConcurrentReferenceHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.ThreadUtil.createThreadPoolName;
import static java.lang.String.format;

/**
 * The {@link MetricsRegistry} implementation.
 */
public class MetricsRegistryImpl implements MetricsRegistry {

    private static final Comparator<ProbeInstance> COMPARATOR = new Comparator<ProbeInstance>() {
        @Override
        public int compare(ProbeInstance o1, ProbeInstance o2) {
            return o1.name.compareTo(o2.name);
        }
    };

    final ILogger logger;
    private final ProbeLevel minimumLevel;

    private final ScheduledExecutorService scheduledExecutorService;
    private final ConcurrentMap<String, ProbeInstance> probeInstances = new ConcurrentHashMap<String, ProbeInstance>();

    // use ConcurrentReferenceHashMap to allow unreferenced Class instances to be garbage collected
    private final ConcurrentMap<Class<?>, SourceMetadata> metadataMap
            = new ConcurrentReferenceHashMap<Class<?>, SourceMetadata>();
    private final LockStripe lockStripe = new LockStripe();

    private final AtomicReference<SortedProbeInstances> sortedProbeInstancesRef
            = new AtomicReference<SortedProbeInstances>(new SortedProbeInstances(0));


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

        scheduledExecutorService = new ScheduledThreadPoolExecutor(2,
                new ThreadFactoryImpl(createThreadPoolName(name, "MetricsRegistry")));

        if (logger.isFinestEnabled()) {
            logger.finest("MetricsRegistry minimumLevel:" + minimumLevel);
        }
    }

    @Override
    public ProbeLevel minimumLevel() {
        return minimumLevel;
    }

    long modCount() {
        return sortedProbeInstancesRef.get().mod;
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
    public <S> void register(S source, String name, ProbeLevel level, LongProbeFunction<S> function) {
        checkNotNull(source, "source can't be null");
        checkNotNull(name, "name can't be null");
        checkNotNull(function, "function can't be null");
        checkNotNull(level, "level can't be null");

        registerInternal(source, name, level, function);
    }

    @Override
    public <S> void register(S source, String name, ProbeLevel level, DoubleProbeFunction<S> function) {
        checkNotNull(source, "source can't be null");
        checkNotNull(name, "name can't be null");
        checkNotNull(function, "function can't be null");
        checkNotNull(level, "level can't be null");

        registerInternal(source, name, level, function);
    }

    ProbeInstance getProbeInstance(String name) {
        checkNotNull(name, "name can't be null");

        return probeInstances.get(name);
    }

    <S> void registerInternal(S source, String name, ProbeLevel probeLevel, ProbeFunction function) {
        if (!probeLevel.isEnabled(minimumLevel)) {
            return;
        }

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

        incrementMod();
    }

    private void incrementMod() {
        for (; ; ) {
            SortedProbeInstances current = sortedProbeInstancesRef.get();
            SortedProbeInstances update = new SortedProbeInstances(current.mod + 1);
            if (sortedProbeInstancesRef.compareAndSet(current, update)) {
                break;
            }
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
    public <S> void deregister(S source) {
        if (source == null) {
            return;
        }

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
            incrementMod();
        }
    }

    @Override
    public void render(ProbeRenderer renderer) {
        checkNotNull(renderer, "renderer can't be null");

        for (ProbeInstance probeInstance : getSortedProbeInstances()) {
            render(renderer, probeInstance);
        }
    }

    @Override
    public void collectMetrics(Object... objects) {
        for (Object object : objects) {
            if (object instanceof MetricsProvider) {
                ((MetricsProvider) object).provideMetrics(this);
            }
        }
    }

    @Override
    public void discardMetrics(Object... objects) {
        for (Object object : objects) {
            if (object instanceof DiscardableMetricsProvider) {
                ((DiscardableMetricsProvider) object).discardMetrics(this);
            }
        }
    }

    List<ProbeInstance> getSortedProbeInstances() {
        for (; ; ) {
            SortedProbeInstances current = sortedProbeInstancesRef.get();
            if (current.probeInstances != null) {
                return current.probeInstances;
            }

            List<ProbeInstance> probeInstanceList = new ArrayList<ProbeInstance>(probeInstances.values());
            Collections.sort(probeInstanceList, COMPARATOR);

            SortedProbeInstances update = new SortedProbeInstances(current.mod, probeInstanceList);
            if (sortedProbeInstancesRef.compareAndSet(current, update)) {
                return update.probeInstances;
            }
        }
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

    private static class SortedProbeInstances {
        private final long mod;
        private final List<ProbeInstance> probeInstances;

        private SortedProbeInstances(long mod) {
            this.mod = mod;
            this.probeInstances = null;
        }

        SortedProbeInstances(long mod, List<ProbeInstance> probeInstances) {
            this.mod = mod;
            this.probeInstances = probeInstances;
        }
    }

    @Override
    public ProbeBuilder newProbeBuilder() {
        return new ProbeBuilderImpl(this);
    }
}
