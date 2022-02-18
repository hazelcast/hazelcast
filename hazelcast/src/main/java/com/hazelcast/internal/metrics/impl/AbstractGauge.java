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

import com.hazelcast.internal.metrics.Metric;
import com.hazelcast.internal.metrics.ProbeFunction;

import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicReference;

abstract class AbstractGauge implements Metric {

    protected final MetricsRegistryImpl metricsRegistry;
    protected final String name;
    protected long lastCollectionId = Long.MIN_VALUE;

    AbstractGauge(MetricsRegistryImpl metricsRegistry, String name) {
        this.metricsRegistry = metricsRegistry;
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    public abstract void onProbeInstanceSet(ProbeInstance probeInstance);

    abstract MetricValueCatcher getCatcherOrNull();

    final void onCollectionCompleted(long collectionId) {
        if (lastCollectionId != collectionId) {
            MetricValueCatcher catcher = getCatcherOrNull();
            if (catcher != null && catcher instanceof AbstractMetricValueCatcher) {
                ((AbstractMetricValueCatcher) catcher).clearCachedValue();
                ((AbstractMetricValueCatcher) catcher).clearCachedMetricSourceRef();
            }
        }
    }

    abstract class AbstractMetricValueCatcher implements MetricValueCatcher {
        private final AtomicReference<DynamicMetricSourceReference> dynamicMetricRef = new AtomicReference<>();

        @Override
        public final void catchMetricValue(long collectionId, Object source, ProbeFunction function) {
            lastCollectionId = collectionId;
            if (function == null || source == null) {
                clearCachedValue();
            } else {
                DynamicMetricSourceReference cachedMetricRef = dynamicMetricRef.get();
                if (cachedMetricRef == null || source != cachedMetricRef.source() || function != cachedMetricRef.function()) {
                    dynamicMetricRef.set(new DynamicMetricSourceReference(source, function));
                    clearCachedValue();
                }
            }
        }

        final <T> T readBase(MetricValueExtractorFunction<T> extractorFn, MetricCachedValueReaderFunction<T> readCachedFn) {
            // called from LongGaugeImpl#read() and DoubleGaugeImpl#read()
            // it does boxing and unboxing through the passed function
            // parameters as the trade-off of the mostly shared code base

            // check if we have a usable cached dynamic metric reference
            // if so, read the value from it
            DynamicMetricSourceReference cachedMetricRef = dynamicMetricRef.get();
            if (cachedMetricRef != null) {
                // need to make local copies since source and function
                // is accessed through weak references
                Object source = cachedMetricRef.source();
                ProbeFunction function = cachedMetricRef.function();

                if (source != null && function != null) {
                    return extractorFn.extractValue(name, source, function, metricsRegistry);
                } else {
                    clearCachedMetricSourceRef();
                }
            }

            // otherwise use the cached value
            return readCachedFn.readCachedValue();
        }

        abstract void clearCachedValue();

        final void clearCachedMetricSourceRef() {
            dynamicMetricRef.lazySet(null);
        }

    }

    private static final class DynamicMetricSourceReference {
        private final WeakReference<Object> sourceRef;
        private final WeakReference<ProbeFunction> functionRef;

        DynamicMetricSourceReference(Object source, ProbeFunction function) {
            this.sourceRef = new WeakReference<>(source);
            this.functionRef = new WeakReference<>(function);
        }

        Object source() {
            return sourceRef.get();
        }

        ProbeFunction function() {
            return functionRef.get();
        }
    }

    @FunctionalInterface
    interface MetricValueExtractorFunction<T> {
        T extractValue(String name, Object source, ProbeFunction function, MetricsRegistryImpl metricsRegistry);
    }

    @FunctionalInterface
    interface MetricCachedValueReaderFunction<T> {
        T readCachedValue();
    }
}
