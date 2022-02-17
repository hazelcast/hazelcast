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

import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeFunction;
import com.hazelcast.internal.util.counters.Counter;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Semaphore;

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

/**
 * A MethodProbe is a {@link ProbeFunction} that invokes a method that is annotated with {@link Probe}.
 */
abstract class MethodProbe implements ProbeFunction {

    private static final Object[] EMPTY_ARGS = new Object[0];

    final Method method;
    final CachedProbe probe;
    final int type;
    final SourceMetadata sourceMetadata;
    final String probeName;

    MethodProbe(Method method, Probe probe, int type, SourceMetadata sourceMetadata) {
        this.method = method;
        this.probe = new CachedProbe(probe);
        this.type = type;
        this.sourceMetadata = sourceMetadata;
        this.probeName = probe.name();
        assert probeName != null;
        assert probeName.length() > 0;
        method.setAccessible(true);

    }

    void register(MetricsRegistryImpl metricsRegistry, Object source, String namePrefix) {
        MetricDescriptor descriptor = metricsRegistry
                .newMetricDescriptor()
                .withPrefix(namePrefix)
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
        int type = getType(method.getReturnType());
        if (type == -1) {
            throw new IllegalArgumentException(format("@Probe method '%s.%s() has an unsupported return type'",
                    method.getDeclaringClass().getName(), method.getName()));
        }

        if (method.getParameterTypes().length != 0) {
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

        LongMethodProbe(Method method, Probe probe, int type, SourceMetadata sourceMetadata) {
            super(method, probe, type, sourceMetadata);
        }

        @Override
        public long get(S source) throws Exception {
            switch (type) {
                case TYPE_PRIMITIVE_LONG:
                    return ((Number) method.invoke(source, EMPTY_ARGS)).longValue();
                case TYPE_LONG_NUMBER:
                    Number longNumber = (Number) method.invoke(source, EMPTY_ARGS);
                    return longNumber == null ? 0 : longNumber.longValue();
                case TYPE_MAP:
                    Map<?, ?> map = (Map<?, ?>) method.invoke(source, EMPTY_ARGS);
                    return map == null ? 0 : map.size();
                case TYPE_COLLECTION:
                    Collection<?> collection = (Collection<?>) method.invoke(source, EMPTY_ARGS);
                    return collection == null ? 0 : collection.size();
                case TYPE_COUNTER:
                    Counter counter = (Counter) method.invoke(source, EMPTY_ARGS);
                    return counter == null ? 0 : counter.get();
                case TYPE_SEMAPHORE:
                    Semaphore semaphore = (Semaphore) method.invoke(source, EMPTY_ARGS);
                    return semaphore == null ? 0 : semaphore.availablePermits();
                default:
                    throw new IllegalStateException("Unrecognized type:" + type);
            }
        }
    }

    static class DoubleMethodProbe<S> extends MethodProbe implements DoubleProbeFunction<S> {

        DoubleMethodProbe(Method method, Probe probe, int type, SourceMetadata sourceMetadata) {
            super(method, probe, type, sourceMetadata);
        }

        @Override
        public double get(S source) throws Exception {
            switch (type) {
                case TYPE_DOUBLE_PRIMITIVE:
                case TYPE_DOUBLE_NUMBER:
                    Number result = (Number) method.invoke(source, EMPTY_ARGS);
                    return result == null ? 0 : result.doubleValue();
                default:
                    throw new IllegalStateException("Unrecognized type:" + type);
            }
        }
    }
}
