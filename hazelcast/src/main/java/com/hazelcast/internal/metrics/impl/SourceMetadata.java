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

import com.hazelcast.internal.metrics.ExcludedMetricTargets;
import com.hazelcast.internal.metrics.MetricTarget;
import com.hazelcast.internal.metrics.Probe;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.internal.metrics.impl.FieldProbe.createFieldProbe;
import static com.hazelcast.internal.metrics.impl.MethodProbe.createMethodProbe;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.flatten;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

/**
 * Contains the metadata for an object with @Probe fields/methods.
 *
 * This object is effectively immutable after construction.
 */
final class SourceMetadata {
    private final List<FieldProbe> fields = new ArrayList<>();
    private final List<MethodProbe> methods = new ArrayList<>();
    private final Collection<MetricTarget> excludedTargetsClass;

    SourceMetadata(Class clazz) {
        // we scan all the methods/fields of the class/interface hierarchy.
        List<Class<?>> classList = new ArrayList<>();
        flatten(clazz, classList);

        for (Class flattenedClass : classList) {
            scanFields(flattenedClass);
            scanMethods(flattenedClass);
        }

        excludedTargetsClass = getTypeExcludedTarget(clazz);
    }

    void register(MetricsRegistryImpl metricsRegistry, Object source, String namePrefix) {
        for (FieldProbe field : fields) {
            field.register(metricsRegistry, source, namePrefix);
        }

        for (MethodProbe method : methods) {
            method.register(metricsRegistry, source, namePrefix);
        }
    }

    private Collection<MetricTarget> getTypeExcludedTarget(Class clazz) {
        Collection<MetricTarget> typeExclusions;
        ExcludedMetricTargets targetsAnnotation = (ExcludedMetricTargets) clazz.getAnnotation(ExcludedMetricTargets.class);
        if (targetsAnnotation != null) {
            typeExclusions = unmodifiableList(asList(targetsAnnotation.value()));
        } else {
            typeExclusions = emptyList();
        }
        return typeExclusions;
    }

    private void scanFields(Class<?> clazz) {
        for (Field field : clazz.getDeclaredFields()) {
            Probe probe = field.getAnnotation(Probe.class);

            if (probe == null) {
                continue;
            }

            FieldProbe fieldProbe = createFieldProbe(field, probe, this);
            fields.add(fieldProbe);
        }
    }

    private void scanMethods(Class<?> clazz) {
        for (Method method : clazz.getDeclaredMethods()) {
            Probe probe = method.getAnnotation(Probe.class);

            if (probe == null) {
                continue;
            }

            MethodProbe methodProbe = createMethodProbe(method, probe, this);
            methods.add(methodProbe);
        }
    }

    /**
     * Don't modify the returned list!
     */
    public List<FieldProbe> fields() {
        return fields;
    }

    /**
     * Don't modify the returned list!
     */
    public List<MethodProbe> methods() {
        return methods;
    }

    /**
     * Excluded metric targets defined on the class this instance created for.
     */
    public Collection<MetricTarget> excludedTargetsClass() {
        return excludedTargetsClass;
    }
}
