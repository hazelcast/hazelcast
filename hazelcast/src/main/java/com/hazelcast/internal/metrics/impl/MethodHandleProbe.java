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

import static com.hazelcast.internal.metrics.impl.ProbeType.getType;
import static java.lang.String.format;

import org.lanternpowered.lmbda.LambdaFactory;
import org.lanternpowered.lmbda.LambdaType;

import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeFunction;
import com.hazelcast.internal.util.counters.Counter;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.WrongMethodTypeException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

abstract class MethodHandleProbe<S> implements ProbeFunction {
    protected static final Lookup LOOKUP = MethodHandles.lookup();

    protected final boolean isStatic;
    protected final CachedProbe probe;
    protected final ProbeType type;
    protected final SourceMetadata sourceMetadata;
    protected final String probeName;

    @Nullable
    protected Function<S, Object> objectGetter;
    @Nullable
    protected Supplier<Object> objectStaticGetter;

    protected MethodHandleProbe(MethodHandle getterMethod, boolean isStatic, Probe probe, ProbeType type,
            SourceMetadata sourceMetadata) {
        this.probe = new CachedProbe(probe);
        this.type = type;
        this.sourceMetadata = sourceMetadata;

        probeName = probe.name();
        assert probeName != null;
        assert probeName.length() > 0;

        this.isStatic = isStatic;

        try {
            if (type.isPrimitive()) {
                populatePrimitiveGetters(getterMethod);
            } else {
                if (isStatic) {
                    objectStaticGetter = LambdaFactory.create(new LambdaType<Supplier<Object>>() {
                    }, getterMethod);
                } else {
                    objectGetter = LambdaFactory.create(new LambdaType<Function<S, Object>>() {
                    }, getterMethod);
                }
            }
        } catch (

        IllegalStateException e) {
            if (e.getCause() instanceof WrongMethodTypeException) {
                throw new IllegalArgumentException(e.getCause());
            } else {
                throw e;
            }
        }
    }

    abstract void populatePrimitiveGetters(MethodHandle getterMethod);

    void register(MetricsRegistryImpl metricsRegistry, Object source, String namePrefix) {
        MetricDescriptor descriptor = metricsRegistry.newMetricDescriptor().withPrefix(namePrefix).withMetric(getProbeName());
        metricsRegistry.registerInternal(source, descriptor, probe.level(), this);
    }

    void register(MetricsRegistryImpl metricsRegistry, MetricDescriptor descriptor, Object source) {
        metricsRegistry.registerStaticProbe(source, descriptor, getProbeName(), probe.level(), probe.unit(), this);
    }

    String getProbeName() {
        return probeName;
    }

    static <S> MethodHandleProbe<S> createProbe(MethodHandle getterMethod, boolean isStatic, Probe probe,
            SourceMetadata sourceMetadata) {
        ProbeType type = getType(getterMethod.type().returnType());
        if (type == null) {
            throw new IllegalArgumentException(format("@Probe '%s' is of an unhandled type", getterMethod));
        }

        if (type.getMapsTo() == double.class) {
            return new DoubleMethodHandleProbe<>(getterMethod, isStatic, probe, type, sourceMetadata);
        } else if (type.getMapsTo() == long.class) {
            return new LongMethodHandleProbe<>(getterMethod, isStatic, probe, type, sourceMetadata);
        } else {
            throw new IllegalArgumentException(type.toString());
        }
    }

    static class LongMethodHandleProbe<S> extends MethodHandleProbe<S> implements LongProbeFunction<S> {
        @Nullable
        private ToLongFunction<S> primitiveGetter;
        @Nullable
        private LongSupplier primitiveStaticGetter;

        LongMethodHandleProbe(MethodHandle getterMethod, boolean isStatic, Probe probe, ProbeType type,
                SourceMetadata sourceMetadata) {
            super(getterMethod, isStatic, probe, type, sourceMetadata);
        }

        @Override
        void populatePrimitiveGetters(MethodHandle getterMethod) {
            if (isStatic) {
                primitiveStaticGetter = LambdaFactory.create(new LambdaType<LongSupplier>() {
                }, getterMethod);
            } else {
                primitiveGetter = LambdaFactory.create(new LambdaType<ToLongFunction<S>>() {
                }, getterMethod);
            }
        }

        @Override
        public long get(S source) throws Exception {
            switch (type) {
                case TYPE_LONG_PRIMITIVE:
                    return isStatic ? primitiveStaticGetter.getAsLong() : primitiveGetter.applyAsLong(source);
                case TYPE_LONG_NUMBER:
                    Number longNumber = getObject(source);
                    return longNumber == null ? 0 : longNumber.longValue();
                case TYPE_MAP:
                    Map<?, ?> map = getObject(source);
                    return map == null ? 0 : map.size();
                case TYPE_COLLECTION:
                    Collection<?> collection = getObject(source);
                    return collection == null ? 0 : collection.size();
                case TYPE_COUNTER:
                    Counter counter = getObject(source);
                    return counter == null ? 0 : counter.get();
                case TYPE_SEMAPHORE:
                    Semaphore semaphore = getObject(source);
                    return semaphore == null ? 0 : semaphore.availablePermits();
                default:
                    throw new IllegalStateException("Unrecognized type: " + type);
            }
        }
    }

    static class DoubleMethodHandleProbe<S> extends MethodHandleProbe<S> implements DoubleProbeFunction<S> {
        @Nullable
        private ToDoubleFunction<S> primitiveGetter;
        @Nullable
        private DoubleSupplier primitiveStaticGetter;

        DoubleMethodHandleProbe(MethodHandle getterMethod, boolean isStatic, Probe probe, ProbeType type,
                SourceMetadata sourceMetadata) {
            super(getterMethod, isStatic, probe, type, sourceMetadata);
        }

        @Override
        void populatePrimitiveGetters(MethodHandle getterMethod) {
            if (isStatic) {
                primitiveStaticGetter = LambdaFactory.create(new LambdaType<DoubleSupplier>() {
                }, getterMethod);
            } else {
                primitiveGetter = LambdaFactory.create(new LambdaType<ToDoubleFunction<S>>() {
                }, getterMethod);
            }

        }

        @Override
        public double get(S source) throws Exception {
            switch (type) {
                case TYPE_DOUBLE_PRIMITIVE:
                    return isStatic ? primitiveStaticGetter.getAsDouble() : primitiveGetter.applyAsDouble(source);
                case TYPE_DOUBLE_NUMBER:
                    Number doubleNumber = getObject(source);
                    return doubleNumber == null ? 0 : doubleNumber.doubleValue();
                default:
                    throw new IllegalStateException("Unrecognized type: " + type);
            }
        }

    }

    protected <T> T getObject(S source) {
        return (T) (isStatic ? objectStaticGetter.get() : objectGetter.apply(source));
    }
}
