/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_COLLECTION;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_COUNTER;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_DOUBLE_NUMBER;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_DOUBLE_PRIMITIVE;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_LONG_NUMBER;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_MAP;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_PRIMITIVE_LONG;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_SEMAPHORE;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.getType;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.isDouble;
import static java.lang.String.format;

import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeFunction;
import com.hazelcast.internal.util.counters.Counter;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * A MethodProbe is a {@link ProbeFunction} that invokes a method that is annotated with {@link Probe}.
 */
abstract class MethodProbe implements ProbeFunction {
    private static final Lookup LOOKUP = MethodHandles.lookup();

    final MethodHandle method;
    final boolean isMethodStatic;
    final CachedProbe probe;
    final int type;
    final SourceMetadata sourceMetadata;
    final String probeName;

    MethodProbe(final Method method, final Probe probe, final int type, final SourceMetadata sourceMetadata) {
        try {
            method.setAccessible(true);
            this.method = LOOKUP.unreflect(method);
            isMethodStatic = Modifier.isStatic(method.getModifiers());
            this.probe = new CachedProbe(probe);
            this.type = type;
            this.sourceMetadata = sourceMetadata;
            probeName = probe.name();
            assert probeName != null;
            assert probeName.length() > 0;
        } catch (final ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    void register(final MetricsRegistryImpl metricsRegistry, final Object source, final String namePrefix) {
        final MetricDescriptor descriptor = metricsRegistry.newMetricDescriptor().withPrefix(namePrefix)
                .withMetric(getProbeName());
        metricsRegistry.registerInternal(source, descriptor, probe.level(), this);
    }

    void register(final MetricsRegistryImpl metricsRegistry, final MetricDescriptor descriptor, final Object source) {
        metricsRegistry.registerStaticProbe(source, descriptor, getProbeName(), probe.level(), probe.unit(), this);
    }

    String getProbeName() {
        return probeName;
    }

    static <S> MethodProbe createMethodProbe(final Method method, final Probe probe, final SourceMetadata sourceMetadata) {
        final int type = getType(method.getReturnType());
        if (type == -1) {
            throw new IllegalArgumentException(format("@Probe method '%s.%s() has an unsupported return type'",
                    method.getDeclaringClass().getName(), method.getName()));
        }

        if (method.getParameterCount() != 0) {
            throw new IllegalArgumentException(format("@Probe method '%s.%s' can't have arguments",
                    method.getDeclaringClass().getName(), method.getName()));
        }

        if (isDouble(type)) {
            return new DoubleMethodProbe<S>(method, probe, type, sourceMetadata);
        } else {
            return new LongMethodProbe<S>(method, probe, type, sourceMetadata);
        }
    }

    static class LongMethodProbe<S> extends MethodProbe implements LongProbeFunction<S> {
        LongMethodProbe(final Method method, final Probe probe, final int type, final SourceMetadata sourceMetadata) {
            super(method, probe, type, sourceMetadata);
        }

        @Override
        public long get(final S source) throws Exception {
            switch (type) {
                case TYPE_PRIMITIVE_LONG:
                case TYPE_LONG_NUMBER:
                    final Number longNumber = invoke(source);
                    return longNumber == null ? 0 : longNumber.longValue();
                case TYPE_MAP:
                    final Map<?, ?> map = invoke(source);
                    return map == null ? 0 : map.size();
                case TYPE_COLLECTION:
                    final Collection<?> collection = invoke(source);
                    return collection == null ? 0 : collection.size();
                case TYPE_COUNTER:
                    final Counter counter = invoke(source);
                    return counter == null ? 0 : counter.get();
                case TYPE_SEMAPHORE:
                    final Semaphore semaphore = invoke(source);
                    return semaphore == null ? 0 : semaphore.availablePermits();
                default:
                    throw new IllegalStateException("Unrecognized type: " + type);
            }
        }
    }

    static class DoubleMethodProbe<S> extends MethodProbe implements DoubleProbeFunction<S> {
        DoubleMethodProbe(final Method method, final Probe probe, final int type, final SourceMetadata sourceMetadata) {
            super(method, probe, type, sourceMetadata);
        }

        @Override
        public double get(final S source) throws Exception {
            switch (type) {
                case TYPE_DOUBLE_PRIMITIVE:
                case TYPE_DOUBLE_NUMBER:
                    final Number result = invoke(source);
                    return result == null ? 0 : result.doubleValue();
                default:
                    throw new IllegalStateException("Unrecognized type: " + type);
            }
        }
    }

    protected <T> T invoke(final Object source) throws Exception {
        try {
            if (isMethodStatic) {
                return (T) method.invoke();
            } else {
                return (T) method.invoke(source);
            }
        } catch (final Exception e) {
            throw e;
        } catch (final Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
