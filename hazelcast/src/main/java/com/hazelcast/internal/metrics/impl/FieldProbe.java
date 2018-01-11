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

import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeFunction;
import com.hazelcast.internal.util.counters.Counter;

import java.lang.reflect.Field;
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
 * A FieldProbe is a {@link ProbeFunction} that reads out a field that is annotated with {@link Probe}.
 */
abstract class FieldProbe implements ProbeFunction {

    final Probe probe;
    final Field field;
    final int type;

    FieldProbe(Field field, Probe probe, int type) {
        this.field = field;
        this.probe = probe;
        this.type = type;
        field.setAccessible(true);
    }

    void register(MetricsRegistryImpl metricsRegistry, Object source, String namePrefix) {
        String name = getName(namePrefix);
        metricsRegistry.registerInternal(source, name, probe.level(), this);
    }

    private String getName(String namePrefix) {
        String name = field.getName();
        if (!probe.name().equals("")) {
            name = probe.name();
        }

        return namePrefix + "." + name;
    }

    static <S> FieldProbe createFieldProbe(Field field, Probe probe) {
        int type = getType(field.getType());
        if (type == -1) {
            throw new IllegalArgumentException(format("@Probe field '%s' is of an unhandled type", field));
        }

        if (isDouble(type)) {
            return new DoubleFieldProbe<S>(field, probe, type);
        } else {
            return new LongFieldProbe<S>(field, probe, type);
        }
    }

    static class LongFieldProbe<S> extends FieldProbe implements LongProbeFunction<S> {

        public LongFieldProbe(Field field, Probe probe, int type) {
            super(field, probe, type);
        }

        @Override
        public long get(S source) throws Exception {
            switch (type) {
                case TYPE_PRIMITIVE_LONG:
                    return field.getLong(source);
                case TYPE_LONG_NUMBER:
                    Number longNumber = (Number) field.get(source);
                    return longNumber == null ? 0 : longNumber.longValue();
                case TYPE_MAP:
                    Map<?, ?> map = (Map<?, ?>) field.get(source);
                    return map == null ? 0 : map.size();
                case TYPE_COLLECTION:
                    Collection<?> collection = (Collection<?>) field.get(source);
                    return collection == null ? 0 : collection.size();
                case TYPE_COUNTER:
                    Counter counter = (Counter) field.get(source);
                    return counter == null ? 0 : counter.get();
                case TYPE_SEMAPHORE:
                    Semaphore semaphore = (Semaphore) field.get(source);
                    return semaphore == null ? 0 : semaphore.availablePermits();
                default:
                    throw new IllegalStateException("Unhandled type:" + type);
            }
        }
    }

    static class DoubleFieldProbe<S> extends FieldProbe implements DoubleProbeFunction<S> {

        public DoubleFieldProbe(Field field, Probe probe, int type) {
            super(field, probe, type);
        }

        @Override
        public double get(S source) throws Exception {
            switch (type) {
                case TYPE_DOUBLE_PRIMITIVE:
                    return field.getDouble(source);
                case TYPE_DOUBLE_NUMBER:
                    Number doubleNumber = (Number) field.get(source);
                    return doubleNumber == null ? 0 : doubleNumber.doubleValue();
                default:
                    throw new IllegalStateException("Unhandled type:" + type);
            }
        }
    }
}
