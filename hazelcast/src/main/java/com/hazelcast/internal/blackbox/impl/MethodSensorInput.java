/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.blackbox.impl;

import com.hazelcast.internal.blackbox.SensorInput;
import com.hazelcast.util.counters.Counter;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;

import static java.lang.String.format;

/**
 * A sensor input that reads out a method that is annotated with {@link SensorInput}.
 */
final class MethodSensorInput extends AccessibleObjectInput {

    private final Method method;
    private final SensorInput sensorInput;
    private final int type;

    MethodSensorInput(Method method, SensorInput sensorInput) {
        this.method = method;
        this.sensorInput = sensorInput;
        checkNoArguments();
        this.type = getType();
        method.setAccessible(true);
    }

    private void checkNoArguments() {
        if (method.getParameterTypes().length != 0) {
            throw new IllegalArgumentException(format("@SensorInput method '%s.%s' can't have arguments",
                    method.getDeclaringClass().getName(), method.getName()));
        }
    }

    private int getType() {
        int type = getType(method.getReturnType());
        if (type != -1) {
            return type;
        }

        throw new IllegalArgumentException(format("@SensorInput method '%s.%s() has an unsupported return type'",
                method.getDeclaringClass().getName(), method.getName()));
    }

    void register(BlackboxImpl blackbox, Object source, String parameterPrefix) {
        String parameter = getParameter(parameterPrefix);
        blackbox.registerInternal(source, parameter, this);
    }

    private String getParameter(String parameterPrefix) {
        String sensorName = method.getName();
        if (!sensorInput.name().equals("")) {
            sensorName = sensorInput.name();
        }

        return parameterPrefix + "." + sensorName;
    }

    @Override
    public boolean isDouble() {
        return isDouble(type);
    }

    @Override
    public long getLong(Object source) throws Exception {
        switch (type) {
            case TYPE_PRIMITIVE_LONG:
                return ((Number) method.invoke(source)).longValue();
            case TYPE_LONG_NUMBER:
                Number longNumber = (Number) method.invoke(source);
                return longNumber == null ? 0 : longNumber.longValue();
            case TYPE_MAP:
                Map<?, ?> map = (Map<?, ?>) method.invoke(source);
                return map == null ? 0 : map.size();
            case TYPE_COLLECTION:
                Collection<?> collection = (Collection<?>) method.invoke(source);
                return collection == null ? 0 : collection.size();
            case TYPE_COUNTER:
                Counter counter = (Counter) method.invoke(source);
                return counter == null ? 0 : counter.get();
            default:
                throw new IllegalStateException("Unrecognized type:" + type);
        }
    }

    @Override
    public double getDouble(Object source) throws Exception {
        switch (type) {
            case TYPE_DOUBLE_PRIMITIVE:
            case TYPE_DOUBLE_NUMBER:
                Number result = (Number) method.invoke(source);
                return result == null ? 0 : result.doubleValue();
            default:
                throw new IllegalStateException("Unrecognized type:" + type);
        }
    }
}
