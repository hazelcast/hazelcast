/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.internal.metrics.impl.ProbeType.getType;
import static java.lang.String.format;

import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeFunction;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.counters.Counter;

import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A {@link MethodProbe} is a {@link ProbeFunction} that invokes a method that is annotated with {@link Probe}.
 * <p>
 * Internally accesses the method using reflection via one of these mechanisms (as a pose to a simple {@link Method#invoke} for
 * <a href="https://github.com/hazelcast/hazelcast/pull/25279">performance reasons</a>):
 * <ul>
 * <li>{@link #methodHandle}
 * <li>{@link #staticAccessor}
 * <li>{@link #nonStaticAccessor}
 * </ul>
 */
abstract class MethodProbe implements ProbeFunction {
    private static final Lookup LOOKUP = MethodHandles.lookup();

    /**
     * {@link MethodHandle} used to access primitives to avoid boxing, specifically to reduce redundant object creation.
     * <p>
     * E.G. for a method that returns {@code long}, when typically invoked via reflection, an {@link Object} would instead be
     * returned. The {@code long} would be boxed to {@link Long}, and then immediately unboxed again back to a {@code long} for
     * returning from {@link LongProbeFunction#get(Object)}.
     * <p>
     * Faster than basic reflection, but slower than a {@link LambdaMetafactory} generated accessor.
     */
    final MethodHandle methodHandle;

    /**
     * A {@link Supplier} used to access static methods returning objects (reference types).
     * <p>
     * Generated via a {@link LambdaMetafactory}, bound to a {@link MethodHandle} derived from {@link #methodHandle}.
     * <p>
     * Faster than basic reflection and {@link MethodHandle#invokeExact()}, but only applicable for reference types.
     *
     * @see <a href= "https://www.optaplanner.org/blog/2018/01/09/JavaReflectionButMuchFaster.html"> Further reading</a>
     */
    final Supplier<?> staticAccessor;
    /**
     * A {@link Function} used to access instance methods returning objects (reference types)
     * <p>
     * {@link Function#apply(Object)}'s parameter is the instance to retrieve the value from.
     *
     * @see #staticAccessor
     */
    final Function<Object, ?> nonStaticAccessor;

    final boolean isMethodStatic;
    final CachedProbe probe;
    final ProbeType type;
    final SourceMetadata sourceMetadata;
    final String probeName;

    @SuppressWarnings("checkstyle:executablestatementcount")
    MethodProbe(Method method, Probe probe, ProbeType type, SourceMetadata sourceMetadata) {
        try {
            method.setAccessible(true);
            final MethodHandle unreflected = LOOKUP.unreflect(method);

            isMethodStatic = Modifier.isStatic(method.getModifiers());

            if (type.isPrimitive()) {
                // Modify return types to support invokeExact
                MethodType methodType = unreflected.type().changeReturnType(type.getMapsTo());

                if (!isMethodStatic) {
                    methodType = methodType.changeParameterType(0, Object.class);
                }

                methodHandle = unreflected.asType(methodType);
                staticAccessor = null;
                nonStaticAccessor = null;
            } else {
                methodHandle = null;

                final Lookup privateLookup = MethodHandles.privateLookupIn(method.getDeclaringClass(), LOOKUP);
                final MethodType methodReturnType = MethodType.methodType(method.getReturnType());
                final MethodHandle implementation;
                final MethodType dynamicMethodType = unreflected.type();

                if (isMethodStatic) {
                    implementation = privateLookup.findStatic(method.getDeclaringClass().getClass(), method.getName(),
                            methodReturnType);

                    staticAccessor = (Supplier<?>) LambdaMetafactory
                            .metafactory(privateLookup, "get", MethodType.methodType(Supplier.class),
                                    MethodType.methodType(Object.class, new Class<?>[0]), implementation, dynamicMethodType)
                            .getTarget().invokeExact();
                    nonStaticAccessor = null;
                } else {
                    implementation = privateLookup.findVirtual(method.getDeclaringClass(), method.getName(), methodReturnType);

                    staticAccessor = null;
                    nonStaticAccessor = (Function<Object, ?>) LambdaMetafactory
                            .metafactory(privateLookup, "apply", MethodType.methodType(Function.class),
                                    MethodType.methodType(Object.class, Object.class), implementation, dynamicMethodType)
                            .getTarget().invokeExact();
                }
            }

            this.probe = new CachedProbe(probe);
            this.type = type;
            this.sourceMetadata = sourceMetadata;
            probeName = probe.name();
            assert probeName != null;
            assert probeName.length() > 0;
        } catch (final Throwable t) {
            throw new RuntimeException(t);
        }
    }

    void register(MetricsRegistryImpl metricsRegistry, Object source, String namePrefix) {
        final MetricDescriptor descriptor = metricsRegistry.newMetricDescriptor().withPrefix(namePrefix)
                .withMetric(getProbeName());
        metricsRegistry.registerInternal(source, descriptor, probe.level(), this);
    }

    void register(MetricsRegistryImpl metricsRegistry, MetricDescriptor descriptor, Object source) {
        metricsRegistry.registerStaticProbe(source, descriptor, getProbeName(), probe.level(), probe.unit(), this);
    }

    String getProbeName() {
        return probeName;
    }

    static <S> MethodProbe createMethodProbe(Method method, Probe probe, SourceMetadata sourceMetadata) {
        final ProbeType type = getType(method.getReturnType());
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
        LongMethodProbe(Method method, Probe probe, ProbeType type, SourceMetadata sourceMetadata) {
            super(method, probe, type, sourceMetadata);
        }

        @SuppressWarnings("checkstyle:CyclomaticComplexity")
        @Override
        public long get(final S source) throws Exception {
            try {
                switch (type) {
                    case TYPE_LONG_PRIMITIVE:
                        return isMethodStatic ? (long) methodHandle.invokeExact() : (long) methodHandle.invokeExact(source);
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
            } catch (final Exception e) {
                throw e;
            } catch (final Throwable t) {
                throw ExceptionUtil.sneakyThrow(t);
            }
        }
    }

    static class DoubleMethodProbe<S> extends MethodProbe implements DoubleProbeFunction<S> {
        DoubleMethodProbe(Method method, Probe probe, ProbeType type, SourceMetadata sourceMetadata) {
            super(method, probe, type, sourceMetadata);
        }

        @Override
        public double get(final S source) throws Exception {
            try {
                switch (type) {
                    case TYPE_DOUBLE_PRIMITIVE:
                        return isMethodStatic ? (double) methodHandle.invokeExact() : (double) methodHandle.invokeExact(source);
                    case TYPE_DOUBLE_NUMBER:
                        final Number result = invoke(source);
                        return result == null ? 0 : result.doubleValue();
                    default:
                        throw new IllegalStateException("Unrecognized type: " + type);
                }
            } catch (final Exception e) {
                throw e;
            } catch (final Throwable t) {
                throw ExceptionUtil.sneakyThrow(t);
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> T invoke(final Object source) {
        return isMethodStatic ? (T) staticAccessor.get() : (T) nonStaticAccessor.apply(source);
    }
}
