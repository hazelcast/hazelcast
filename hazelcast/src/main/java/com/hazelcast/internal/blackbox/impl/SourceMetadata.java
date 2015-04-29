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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.blackbox.impl.AccessibleObjectInput.flatten;

/**
 * Contains the metadata for an object with @SensorInput fields/methods.
 *
 * This object is effectively immutable after it is constructed.
 */
final class SourceMetadata {
    private final List<FieldSensorInput> fields = new ArrayList<FieldSensorInput>();
    private final List<MethodSensorInput> methods = new ArrayList<MethodSensorInput>();

    SourceMetadata(Class clazz) {
        // we scan all the methods/fields of the class/interface hierarchy.
        List<Class<?>> classList = new ArrayList<Class<?>>();
        flatten(clazz, classList);

        for (Class flattenedClass : classList) {
            scanFields(flattenedClass);
            scanMethods(flattenedClass);
        }
    }

    void register(BlackboxImpl blackbox, Object source, String parameterPrefix) {
        for (FieldSensorInput field : fields) {
            field.register(blackbox, source, parameterPrefix);
        }

        for (MethodSensorInput method : methods) {
            method.register(blackbox, source, parameterPrefix);
        }
    }

    void scanFields(Class<?> clazz) {
        for (Field field : clazz.getDeclaredFields()) {
            SensorInput sensorInput = field.getAnnotation(SensorInput.class);

            if (sensorInput == null) {
                continue;
            }

            FieldSensorInput fieldSensorInput = new FieldSensorInput(field, sensorInput);
            fields.add(fieldSensorInput);
        }
    }

    void scanMethods(Class<?> clazz) {
        for (Method method : clazz.getDeclaredMethods()) {
            SensorInput sensorInput = method.getAnnotation(SensorInput.class);

            if (sensorInput == null) {
                continue;
            }

            MethodSensorInput methodSensor = new MethodSensorInput(method, sensorInput);
            methods.add(methodSensor);
        }
    }
}
