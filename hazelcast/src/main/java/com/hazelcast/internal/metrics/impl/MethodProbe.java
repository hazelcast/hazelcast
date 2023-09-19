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

import static com.hazelcast.internal.metrics.impl.ProbeUtils.getType;
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
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * A MethodProbe is a {@link ProbeFunction} that invokes a method that is annotated with {@link Probe}.
 */
abstract class MethodProbe implements ProbeFunction {
    private static final Object[] EMPTY_ARGS = new Object[0];
    private static final Lookup LOOKUP = MethodHandles.lookup();

    final Method method;
    final MethodHandle methodHandle;
    final boolean isMethodStatic;
    final CachedProbe probe;
    final ProbeUtils type;
    final SourceMetadata sourceMetadata;
    final String probeName;

    MethodProbe(final Method method, final Probe probe, final ProbeUtils type, final SourceMetadata sourceMetadata) {
        try {
            this.method = method;
            method.setAccessible(true);
            final MethodHandle unreflected = LOOKUP.unreflect(method);

            isMethodStatic = Modifier.isStatic(method.getModifiers());

            // Modify return types to support invokeExact
            MethodType methodType = unreflected.type().changeReturnType(type.isPrimitive() ? type.getMapsTo() : Object.class);

            if (!isMethodStatic) {
                methodType = methodType.changeParameterType(0, Object.class);
            }

            this.methodHandle = unreflected.asType(methodType);

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
        final ProbeUtils type = getType(method.getReturnType());
        if (type == null) {
            throw new IllegalArgumentException(format("@Probe method '%s.%s() has an unsupported return type'",
                    method.getDeclaringClass().getName(), method.getName()));
        }

        if (method.getParameterCount() != 0) {
            throw new IllegalArgumentException(format("@Probe method '%s.%s' can't have arguments",
                    method.getDeclaringClass().getName(), method.getName()));
        }

        if (type.getMapsTo() == double.class) {
            return new DoubleMethodProbe<S>(method, probe, type, sourceMetadata);
        } else if (type.getMapsTo() == long.class) {
            return new LongMethodProbe<S>(method, probe, type, sourceMetadata);
        } else {
            throw new IllegalArgumentException(type.toString());
        }
    }

    static class LongMethodProbe<S> extends MethodProbe implements LongProbeFunction<S> {
        LongMethodProbe(final Method method, final Probe probe, final ProbeUtils type, final SourceMetadata sourceMetadata) {
            super(method, probe, type, sourceMetadata);
        }

        @Override
        public long get(final S source) throws Throwable {
            switch (type) {
                case TYPE_LONG_PRIMITIVE:
                    return invokeLongPrimitive(source);
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
        DoubleMethodProbe(final Method method, final Probe probe, final ProbeUtils type, final SourceMetadata sourceMetadata) {
            super(method, probe, type, sourceMetadata);
        }

        @Override
        public double get(final S source) throws Throwable {
            switch (type) {
                case TYPE_DOUBLE_PRIMITIVE:
                    return invokeDoublePrimitive(source);
                case TYPE_DOUBLE_NUMBER:
                    final Number result = invoke(source);
                    return result == null ? 0 : result.doubleValue();
                default:
                    throw new IllegalStateException("Unrecognized type: " + type);
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> T invoke(final Object source) throws ReflectiveOperationException {
        // In benchmarking, MethodHandles proved slower with objects
        // Compiling to a CallSite with a LambdaMetafactory is likely to be *much* faster, but struggles to copy with private
        // methods
        return (T) method.invoke(source, EMPTY_ARGS);
    }

    protected double invokeDoublePrimitive(final Object source) throws Throwable {
        return isMethodStatic ? (double) methodHandle.invokeExact() : (double) methodHandle.invokeExact(source);
    }

    protected long invokeLongPrimitive(final Object source) throws Throwable {
        return isMethodStatic ? (long) methodHandle.invokeExact() : (long) methodHandle.invokeExact(source);
    }
}
