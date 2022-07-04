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

package com.hazelcast.internal.metrics.jmx;

import com.hazelcast.internal.util.TriTuple;

import javax.annotation.Nonnull;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.util.TriTuple.of;
import static java.util.stream.Collectors.toCollection;

public class MetricsMBean implements DynamicMBean {

    // we add attributes here while they might be read in parallel by JMX client
    private final ConcurrentMap<String, TriTuple<String, AtomicReference<Number>, Type>> metrics = new ConcurrentHashMap<>();

    /**
     * Adds a metric if necessary and sets its value.
     */
    void setMetricValue(String name, String unit, Number value, Type type) {
        TriTuple<String, AtomicReference<Number>, Type> metricTuple = metrics.get(name);
        if (metricTuple == null) {
            metricTuple = of(unit, new AtomicReference<>(), type);
            metrics.put(name, metricTuple);
        }
        metricTuple.element2.lazySet(value);
    }

    void removeMetric(String name) {
        metrics.remove(name);
    }

    @Override
    public Object getAttribute(String attributeName) throws AttributeNotFoundException {
        TriTuple<String, AtomicReference<Number>, Type> attribute = metrics.get(attributeName);
        if (attribute == null) {
            throw new AttributeNotFoundException(attributeName);
        }
        return attribute.element2.get();
    }

    @Override
    public void setAttribute(Attribute attribute) {
        throw new UnsupportedOperationException("setting attributes is not supported");
    }

    @Override
    public AttributeList getAttributes(String[] attributes) {
        return Arrays.stream(attributes)
                     .filter(metrics::containsKey)
                     .map(a -> uncheckCall(() -> new Attribute(a, getAttribute(a))))
                     .collect(toCollection(AttributeList::new));
    }

    @Override
    public AttributeList setAttributes(AttributeList attributes) {
        throw new UnsupportedOperationException("setting attributes is not supported");
    }

    @Override
    public Object invoke(String actionName, Object[] params, String[] signature) {
        throw new UnsupportedOperationException("invoking is not supported");
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        MBeanAttributeInfo[] array = new MBeanAttributeInfo[metrics.size()];
        int i = 0;
        for (Entry<String, TriTuple<String, AtomicReference<Number>, Type>> entry : metrics.entrySet()) {
            array[i++] = new MBeanAttributeInfo(entry.getKey(), entry.getValue().element3.type,
                    "Unit: " + entry.getValue().element1,
                    true, false, false);
        }

        return new MBeanInfo("Metric", "", array, null, null, null, null);
    }

    int numAttributes() {
        return metrics.size();
    }

    private static <T> T uncheckCall(@Nonnull Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    public enum Type {
        LONG("long"),
        DOUBLE("double");
        private static final Type[] VALUES = values();

        private final String type;

        Type(String type) {
            this.type = type;
        }

        static Type of(String strType) {
            for (Type value : VALUES) {
                if (strType.equals(value.type)) {
                    return value;
                }
            }
            return null;
        }
    }
}
