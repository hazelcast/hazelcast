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

import com.hazelcast.internal.metrics.CollectionCycle;
import com.hazelcast.internal.metrics.DoubleGauge;
import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsCollector;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeBuilder;
import com.hazelcast.internal.metrics.ProbeFunction;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.util.concurrent.ThreadFactoryImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ConcurrentReferenceHashMap;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.ThreadUtil.createThreadPoolName;

/**
 * The {@link MetricsRegistry} implementation.
 */
public class MetricsRegistryImpl implements MetricsRegistry {

    final ILogger logger;

    private final ScheduledExecutorService scheduler;
    private final ConcurrentMap<String, Object[]> roots = new ConcurrentHashMap<String, Object[]>();

    // todo: there is a problem with the ConcurrentReferenceHashMap since it doesnt' cache
    // use ConcurrentReferenceHashMap to allow unreferenced Class instances to be garbage collected
    private final ConcurrentMap<Class<?>, SourceMetadata> metadataMap
            = new ConcurrentReferenceHashMap<Class<?>, SourceMetadata>();

    /**
     * Creates a MetricsRegistryImpl instance.
     *
     * @param logger the ILogger used
     * @throws NullPointerException if logger or minimumLevel is null
     */
    public MetricsRegistryImpl(ILogger logger) {
        this("default", logger);
    }

    /**
     * Creates a MetricsRegistryImpl instance.
     *
     * @param name   Name of the registry
     * @param logger the ILogger used
     * @throws NullPointerException if logger or minimumLevel is null
     */
    public MetricsRegistryImpl(String name, ILogger logger) {
        this.logger = checkNotNull(logger, "logger can't be null");
        this.scheduler = new ScheduledThreadPoolExecutor(2,
                new ThreadFactoryImpl(createThreadPoolName(name, "MetricsRegistry")));
    }

    public Set<String> getNames() {
        Set<String> names = new HashSet<String>(roots.keySet());
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
    public void registerAll(Object... sources) {
        for (Object source : sources) {
            register(source);
        }
    }

    @Override
    public <S> void register(S source) {
        checkNotNull(source, "source can't be null");

        // make sure that the registered source is not without problems.
        if ((source instanceof ProbeInstance)) {
            return;
        }

        if (source instanceof MetricsProvider) {
            ((MetricsProvider) source).provideMetrics(this);
        }

        SourceMetadata sourceMetadata = loadSourceMetadata(source.getClass());
        if (!sourceMetadata.dynamicSource && !sourceMetadata.hasProbes) {
            // todo:for now we ignore; but would be better to throw error
            return;
        }

        String ns = sourceMetadata.ns == null ? "" : sourceMetadata.ns;
        for (; ; ) {
            Object[] old = roots.get(ns);
            if (old == null) {
                Object[] update = new Object[1];
                update[0] = source;
                if (roots.putIfAbsent(ns, update) == null) {
                    return;
                }
            } else {
                Object[] update = new Object[old.length + 1];
                System.arraycopy(old, 0, update, 0, old.length);
                update[update.length - 1] = source;
                if (roots.replace(ns, old, update)) {
                    return;
                }
            }
        }
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

        // todo: we should check for last index of.
        // todo: currently only the roots are scanned for a probe; but the tree
        // isn't traversed.

        Object[] r = roots.get(name);
        if (r == null) {
            int indexOf = name.indexOf('.');
            if (indexOf == -1) {
                return null;
            }

            String prefix = name.substring(0, indexOf);
            Object[] sources = roots.get(prefix);
            if (sources == null) {
                return null;
            }

          //  String probeName = name.substring(indexOf + 1);

            for (Object source : sources) {
                if (source instanceof ProbeInstance) {
                    continue;
                }

                SourceMetadata sourceMetadata = loadSourceMetadata(source.getClass());
                for (AbstractProbe probe : sourceMetadata.probes) {
                    if (probe.name.equals(name)) {
                        return new ProbeInstance(name, source, probe, probe.probe.level());
                    }
                }
            }

            return null;
        } else {
            return (ProbeInstance) r[0];
        }
    }

    <S> void registerInternal(S source, String name, ProbeLevel probeLevel, ProbeFunction function) {
        checkNotNull(source, "source can't be null");

//        // make sure that the registered source is not without problems.
//        if ((source instanceof ProbeInstance)) {
//            return;
//        }

//        if (source instanceof MetricsProvider) {
//            ((MetricsProvider) source).provideMetrics(this);
//        }

//        SourceMetadata sourceMetadata = loadSourceMetadata(source.getClass());
//        if (!sourceMetadata.dynamicSource && !sourceMetadata.hasProbes) {
//            // todo:for now we ignore; but would be better to throw error
//            return;
//        }

         for (; ; ) {
            Object[] old = roots.get(name);
            if (old == null) {
                Object[] update = new Object[1];
                update[0] = source;
                if (roots.putIfAbsent(name, update) == null) {
                    return;
                }
            } else {
                Object[] update = new Object[old.length + 1];
                System.arraycopy(old, 0, update, 0, old.length);
                update[update.length - 1] = source;
                if (roots.replace(name, old, update)) {
                    return;
                }
            }
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

//        for (Map.Entry<String, Object[]> entry : roots.entrySet()) {
//            Object[] array = entry.getValue();
//            int indexOf = -1;
//            for (int k = 0; k < array.length; k++) {
//                if (array[k] == source) {
//                    indexOf = k;
//                    break;
//                }
//            }
//
//            if (indexOf == -1) {
//                // item was not found in the array, so move on to the next.
//                continue;
//            }
//
//            if (array.length == 1) {
//                if (roots.remove(entry.getKey(), array)) {
//                    continue;
//                }
//            }else{
//                Object[] update = ArrayUtils.remove(array, source);
//                if(roots.replace(entry.getKey(), array, update)){
//                    continue;
//                }
//            }
//        }

//
//
//
//        boolean changed = false;
//        for (Map.Entry<String, ProbeInstance> entry : roots.entrySet()) {
//            ProbeInstance probeInstance = entry.getValue();
//
//            if (probeInstance.source != source) {
//                continue;
//            }
//
//            String name = entry.getKey();
//
//            boolean destroyed = false;
//            synchronized (lockStripe.getLock(source)) {
//                if (probeInstance.source == source) {
//                    changed = true;
//                    roots.remove(name);
//                    probeInstance.source = null;
//                    probeInstance.function = null;
//                    destroyed = true;
//                }
//            }
//
//            if (destroyed && logger.isFinestEnabled()) {
//                logger.finest("Destroying probeInstance " + name);
//            }
//        }

//        if (changed) {
//            incrementMod();
//        }
    }

    @Override
    public void collect(MetricsCollector collector, ProbeLevel probeLevel) {
        checkNotNull(collector, "collector can't be null");
        checkNotNull(probeLevel, "probeLevel can't be null");

        CollectionCycle cycle = new CollectionCycleImpl(this, collector, probeLevel);
        for (Map.Entry<String, Object[]> entry : roots.entrySet()) {
            String name = entry.getKey();
            Object[] sources = entry.getValue();

            for (Object source : sources) {
                if (source instanceof ProbeInstance) {
                    ProbeInstance probeInstance = (ProbeInstance) source;
                    probeInstance.collect(collector);
                } else {
                    cycle.collect(name, source);
                }
            }
        }
    }

    @Override
    public void scheduleAtFixedRate(final Runnable publisher, long period, TimeUnit timeUnit) {
        scheduler.scheduleAtFixedRate(publisher, 0, period, timeUnit);
    }

    public void shutdown() {
        // we want to immediately terminate; we don't want to wait till pending tasks have completed.
        scheduler.shutdownNow();
    }

    @Override
    public ProbeBuilder newProbeBuilder() {
        return new ProbeBuilderImpl(this);
    }

}
