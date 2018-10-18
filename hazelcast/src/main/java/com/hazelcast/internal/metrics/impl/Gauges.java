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

import static com.hazelcast.internal.metrics.ProbeUtils.doubleValue;
import static com.hazelcast.internal.metrics.ProbeUtils.toLong;
import static com.hazelcast.util.EmptyStatement.ignore;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import com.hazelcast.internal.metrics.DoubleGauge;
import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.LongProbeFunction;

public final class Gauges {

    private static final Object[] EMPTY_ARGS = new Object[0];

    private Gauges() {
        // utility
    }

    public static LongGauge longGauge() {
        return new ConnectingLongGauge();
    }

    public static DoubleGauge doubleGauge() {
        return new ConnectingDoubleGauge();
    }

    public static void connect(LongGauge unconnected, LongGauge connected) {
        ((ConnectingLongGauge) unconnected).gauge = connected;
    }

    public static void connect(DoubleGauge unconnected, DoubleGauge connected) {
        ((ConnectingDoubleGauge) unconnected).gauge = connected;
    }

    public static LongGauge longGauge(Object supplier, Object source) {
        if (supplier instanceof Method) {
            Method method = (Method) supplier;
            if (LongProbeFunction.class.isAssignableFrom(method.getReturnType())) {
                try {
                    return new LongProbeFunctionGauge((LongProbeFunction) method.invoke(source, EMPTY_ARGS));
                } catch (Exception e) {
                    ignore(e);
                    // use method gauge instead...
                }
            }
            return new LongMethodGauge(method, source);
        }
        if (supplier instanceof Field) {
            Field field = (Field) supplier;
            if (LongProbeFunction.class.isAssignableFrom(field.getType())) {
                try {
                    return new LongProbeFunctionGauge((LongProbeFunction) field.get(source));
                } catch (Exception e) {
                    ignore(e);
                    // use field gauge instead...
                }
            }
            return new LongFieldGauge(field, source);
        }
        if (supplier instanceof LongProbeFunction) {
            return new LongProbeFunctionGauge((LongProbeFunction) supplier);
        }
        throw notSupported(supplier);
    }

    private static UnsupportedOperationException notSupported(Object supplier) {
        return new UnsupportedOperationException("Supplier not supported " + supplier.getClass().getSimpleName());
    }

    public static DoubleGauge doubleGauge(Object supplier, Object source) {
        if (supplier instanceof Method) {
            Method method = (Method) supplier;
            if (DoubleProbeFunction.class.isAssignableFrom(method.getReturnType())) {
                try {
                    return new DoubleProbeFunctionGauge((DoubleProbeFunction) method.invoke(source, EMPTY_ARGS));
                } catch (Exception e) {
                    ignore(e);
                    // use method gauge instead...
                }
            }
            return new DoubleMethodGauge(method, source);
        }
        if (supplier instanceof Field) {
            Field field = (Field) supplier;
            if (DoubleProbeFunction.class.isAssignableFrom(field.getType())) {
                try {
                    return new DoubleProbeFunctionGauge((DoubleProbeFunction) field.get(source));
                } catch (Exception e) {
                    ignore(e);
                    // use field gauge instead...
                }
            }
            return new DoubleFieldGauge(field, source);
        }
        if (supplier instanceof DoubleProbeFunction) {
            return new DoubleProbeFunctionGauge((DoubleProbeFunction) supplier);
        }
        throw notSupported(supplier);
    }

    private static final class ConnectingLongGauge implements LongGauge {

        private LongGauge gauge;

        @Override
        public long read() {
            if (gauge == null) {
                return 0L;
            }
            long v = gauge.read();
            if (v == -1L) {
                gauge = null;
            }
            return v;
        }

    }

    private static final class ConnectingDoubleGauge implements DoubleGauge {

        private DoubleGauge gauge;

        @Override
        public double read() {
            if (gauge == null) {
                return 0d;
            }
            double v = gauge.read();
            if (v == -1d) {
                gauge = null;
            }
            return v;
        }
    }

    private abstract static class AbstractGauge<T> {
        final WeakReference<T>  supplier;
        final WeakReference<Object> source;

        AbstractGauge(T supplier, Object source) {
            this.supplier = new WeakReference<T>(supplier);
            this.source = new WeakReference<Object>(source);
        }
    }

    private abstract static class AbstractLongGauge<T> extends AbstractGauge<T> implements LongGauge {

        AbstractLongGauge(T supplier, Object source) {
            super(supplier, source);
        }

        @Override
        public final long read() {
            T s = supplier.get();
            Object src = source.get();
            if (s == null || src == null) {
                return -1L;
            }
            try {
                return read(s, src);
            } catch (Exception e) {
                return -1L;
            }
        }

        abstract long read(T supplier, Object source) throws Exception;
    }

    private abstract static class AbstractDoubleGauge<T> extends AbstractGauge<T> implements DoubleGauge {

        AbstractDoubleGauge(T supplier, Object source) {
            super(supplier, source);
        }

        @Override
        public final double read() {
            T s = supplier.get();
            Object src = source.get();
            if (s == null || src == null) {
                return -1d;
            }
            try {
                return read(s, src);
            } catch (Exception e) {
                return -1d;
            }
        }

        abstract double read(T supplier, Object source) throws Exception;
    }

    private static final class LongMethodGauge extends AbstractLongGauge<Method> {

        LongMethodGauge(Method supplier, Object source) {
            super(supplier, source);
        }

        @Override
        long read(Method supplier, Object source) throws Exception {
            return toLong(supplier.invoke(source, EMPTY_ARGS));
        }
    }

    private static final class DoubleMethodGauge extends AbstractDoubleGauge<Method> {

        DoubleMethodGauge(Method supplier, Object source) {
            super(supplier, source);
        }

        @Override
        double read(Method supplier, Object source) throws Exception {
            return doubleValue(toLong(supplier.invoke(source, EMPTY_ARGS)));
        }
    }

    private static final class LongFieldGauge extends AbstractLongGauge<Field> {

        LongFieldGauge(Field supplier, Object source) {
            super(supplier, source);
        }

        @Override
        long read(Field supplier, Object source) throws Exception {
            Class<?> type = supplier.getType();
            if (type.isPrimitive()) {
                return supplier.getLong(source);
            }
            return toLong(supplier.get(source));
        }

    }

    private static final class DoubleFieldGauge extends AbstractDoubleGauge<Field> {

        DoubleFieldGauge(Field supplier, Object source) {
            super(supplier, source);
        }

        @Override
        double read(Field supplier, Object source) throws Exception {
            Class<?> type = supplier.getType();
            if (type == double.class || type == float.class) {
                return supplier.getDouble(source);
            }
            return doubleValue(toLong(supplier.get(source)));
        }
    }

    private static final class LongProbeFunctionGauge implements LongGauge {

        private final LongProbeFunction supplier;

        LongProbeFunctionGauge(LongProbeFunction supplier) {
            this.supplier = supplier;
        }

        @Override
        public long read() {
            return supplier.getAsLong();
        }
    }

    private static final class DoubleProbeFunctionGauge implements DoubleGauge {

        private final DoubleProbeFunction supplier;

        DoubleProbeFunctionGauge(DoubleProbeFunction supplier) {
            this.supplier = supplier;
        }

        @Override
        public double read() {
            return supplier.getAsDouble();
        }
    }

}
