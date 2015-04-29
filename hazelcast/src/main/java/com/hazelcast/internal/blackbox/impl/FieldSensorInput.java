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

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;

import static java.lang.String.format;

/**
 * A sensor input that reads out a field that is annotated with {@link SensorInput}.
 */
final class FieldSensorInput extends AccessibleObjectInput {

    private final SensorInput sensorInput;
    private final Field field;
    private final int type;

    FieldSensorInput(Field field, SensorInput sensorInput) {
        this.field = field;
        this.sensorInput = sensorInput;
        this.type = getType();
        field.setAccessible(true);
    }

    private int getType() {
        int type = getType(field.getType());
        if (type != -1) {
            return type;
        }

        throw new IllegalArgumentException(format("@SensorInput field '%s' is of an unhandled type", field));
    }

    @Override
    public boolean isDouble() {
        return isDouble(type);
    }

    void register(BlackboxImpl blackbox, Object source, String parameterPrefix) {
        String parameter = getParameter(parameterPrefix);
        blackbox.registerInternal(source, parameter, this);
    }

    private String getParameter(String parameterPrefix) {
        String sensorName = field.getName();
        if (!sensorInput.name().equals("")) {
            sensorName = sensorInput.name();
        }

        return parameterPrefix + "." + sensorName;
    }

    @Override
    public long getLong(Object source) throws Exception {
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
            default:
                throw new IllegalStateException("Unhandled type:" + type);
        }
    }

    @Override
    public double getDouble(Object source) throws Exception {
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
