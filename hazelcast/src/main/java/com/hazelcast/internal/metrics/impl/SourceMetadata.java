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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.ContainsProbes;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeFunction;
import com.hazelcast.internal.metrics.ProbeName;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.metrics.impl.FieldProbe.createFieldProbe;
import static com.hazelcast.internal.metrics.impl.MethodProbe.createMethodProbe;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.flatten;

/**
 * Contains the metadata for an object with @Probe fields/methods.
 *
 * This object is effectively immutable after construction.
 */
final class SourceMetadata {

    // todo: we need to deal with conflicting ones
    private final Map<String, ProbeFunction> functions = new HashMap<String, ProbeFunction>();
    private final List<Field> traversableFields = new ArrayList<Field>();
    private Method nameMethod;
    private String probeName;

    SourceMetadata(Class clazz) {
        // we scan all the methods/fields of the class/interface hierarchy.
        List<Class<?>> classList = new ArrayList<Class<?>>();
        flatten(clazz, classList);

        for (Class flattenedClass : classList) {
            scanFieldsProbes(flattenedClass);
            scanMethodProbes(flattenedClass);
            scanContainsProbesFields(flattenedClass);
            scanNameMethods(flattenedClass);
            scanCompositeProbe(flattenedClass);
        }
    }

    public Set<String> getProbeNames() {
        return functions.keySet();
    }

    // todo: object name can be cached.
    public String getObjectName(Object instance) {
        if (nameMethod != null) {
            try {
                return (String) nameMethod.invoke(instance);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
        if (probeName != null) {
            return probeName;
        }

        String string = instance.getClass().getSimpleName();
        char[] c = string.toCharArray();
        c[0] = Character.toLowerCase(c[0]);
        return new String(c);
    }

    private void scanNameMethods(Class clazz) {
        for (Method method : clazz.getDeclaredMethods()) {
            ProbeName probeName = method.getAnnotation(ProbeName.class);

            if (probeName == null) {
                continue;
            }
            method.setAccessible(true);
            nameMethod = method;
            return;
        }
    }

    private void scanCompositeProbe(Class clazz) {
//        CompositeProbe compositeProbe  = clazz.getAnnotation(CompositeProbe.class);
//
//
//        for (Method method : clazz.getDeclaredMethods()) {
//            CompositeProbe compositeProbe = method.getAnnotation();
//
//            if (compositeProbe == null) {
//                continue;
//            }
//
//            this.probeName = compositeProbe.name();
//            return;
//        }
    }

    ProbeFunction findFunction(String name) {
        return functions.get(name);
    }

    List<Field> getTraversableFields() {
        return traversableFields;
    }

    void scanFieldsProbes(Class<?> clazz) {
        for (Field field : clazz.getDeclaredFields()) {
            Probe probe = field.getAnnotation(Probe.class);

            if (probe == null) {
                continue;
            }

            FieldProbe fieldProbe = createFieldProbe(field, probe);
            functions.put(fieldProbe.getName(), fieldProbe);
        }
    }

    void scanContainsProbesFields(Class<?> clazz) {
        for (Field field : clazz.getDeclaredFields()) {
            ContainsProbes containsProbes = field.getAnnotation(ContainsProbes.class);

            if (containsProbes == null) {
                continue;
            }

            field.setAccessible(true);
            traversableFields.add(field);
        }
    }

    void scanMethodProbes(Class<?> clazz) {
        for (Method method : clazz.getDeclaredMethods()) {
            Probe probe = method.getAnnotation(Probe.class);

            if (probe == null) {
                continue;
            }

            method.setAccessible(true);
            MethodProbe methodProbe = createMethodProbe(method, probe);
            functions.put(methodProbe.getName(), methodProbe);
        }
    }


}
