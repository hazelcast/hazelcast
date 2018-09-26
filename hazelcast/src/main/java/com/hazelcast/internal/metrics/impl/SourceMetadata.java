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

import com.hazelcast.internal.metrics.MetricSource;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.Namespace;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.metrics.impl.FieldProbe.createFieldProbe;
import static com.hazelcast.internal.metrics.impl.MethodProbe.createMethodProbe;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.flatten;

/**
 * Contains the metadata for an source object with @Probe fields/methods.
 * <p>
 * This object is effectively immutable after construction.
 */
final class SourceMetadata {
    final boolean dynamicSource;
    final boolean hasProbes;
    final String prefix;
    final List<AbstractProbe> probes = new ArrayList<AbstractProbe>();

    SourceMetadata(Class clazz) {
        Namespace p = (Namespace) clazz.getAnnotation(Namespace.class);
        prefix = p == null ? null : p.name();
        // we scan all the methods/fields of the class/interface hierarchy.
        List<Class<?>> classList = new ArrayList<Class<?>>();
        flatten(clazz, classList);

        for (Class flattenedClass : classList) {
            scanFields(flattenedClass);
            scanMethods(flattenedClass);
        }

        //todo: here we can sort the probes in alphabetic order

        this.dynamicSource = MetricSource.class.isAssignableFrom(clazz);
        this.hasProbes = probes.size() > 0;
    }

    private void scanFields(Class<?> clazz) {
        for (Field field : clazz.getDeclaredFields()) {
            Probe probe = field.getAnnotation(Probe.class);
            if (probe != null) {
                FieldProbe fieldProbe = createFieldProbe(prefix, field, probe);
                probes.add(fieldProbe);
            }
        }
    }

    private void scanMethods(Class<?> clazz) {
        for (Method method : clazz.getDeclaredMethods()) {
            Probe probe = method.getAnnotation(Probe.class);
            if (probe != null) {
                MethodProbe methodProbe = createMethodProbe(prefix, method, probe);
                probes.add(methodProbe);
            }
        }
    }
}
