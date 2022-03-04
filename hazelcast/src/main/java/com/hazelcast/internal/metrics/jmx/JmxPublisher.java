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

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsPublisher;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.util.MutableInteger;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

import static com.hazelcast.internal.metrics.MetricTarget.JMX;
import static com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;
import static com.hazelcast.internal.metrics.jmx.MetricsMBean.Type.DOUBLE;
import static com.hazelcast.internal.metrics.jmx.MetricsMBean.Type.LONG;

/**
 * Renderer to create, register and unregister mBeans for metrics as they are
 * rendered.
 */
public class JmxPublisher implements MetricsPublisher {
    private final MBeanServer platformMBeanServer;
    private final String instanceNameEscaped;
    private final String domainPrefix;

    /**
     * key: metric name, value: MetricData
     */
    private final Map<MetricDescriptor, MetricData> metricNameToMetricData = new HashMap<>();
    /**
     * key: jmx object name, value: mBean
     */
    private final Map<ObjectName, MetricsMBean> mBeans = new HashMap<>();

    private volatile boolean isShutdown;
    private final Function<MetricDescriptor, MetricData> createMetricDataFunction;
    private final Function<? super ObjectName, ? extends MetricsMBean> createMBeanFunction;

    public JmxPublisher(String instanceName, String domainPrefix) {
        this.domainPrefix = domainPrefix;
        platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        instanceNameEscaped = escapeObjectNameValue(instanceName);
        createMetricDataFunction = n -> new MetricData(n, instanceNameEscaped, domainPrefix);
        createMBeanFunction = objectName -> {
            MetricsMBean bean = new MetricsMBean();
            try {
                platformMBeanServer.registerMBean(bean, objectName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return bean;
        };
    }

    @Override
    public String name() {
        return "JMX Publisher";
    }

    @Override
    public void publishLong(MetricDescriptor descriptor, long value) {
        publishNumber(descriptor, value, LONG);
    }

    @Override
    public void publishDouble(MetricDescriptor descriptor, double value) {
        publishNumber(descriptor, value, DOUBLE);
    }

    private void publishNumber(MetricDescriptor originalDescriptor, Number value, MetricsMBean.Type type) {
        if (originalDescriptor.isTargetExcluded(JMX)) {
            return;
        }

        final MetricData metricData;
        if (!metricNameToMetricData.containsKey(originalDescriptor)) {
            // we need to take a copy of originalDescriptor here to ensure
            // we map with an instance that doesn't get recycled or mutated
            MetricDescriptor descriptor = copy(originalDescriptor);
            metricData = metricNameToMetricData.computeIfAbsent(descriptor, createMetricDataFunction);
        } else {
            metricData = metricNameToMetricData.get(originalDescriptor);
        }

        assertDoubleRendering(originalDescriptor, metricData, value);

        metricData.wasPresent = true;
        MetricsMBean mBean = mBeans.computeIfAbsent(metricData.objectName, createMBeanFunction);
        if (isShutdown) {
            unregisterMBeanIgnoreError(metricData.objectName);
        }
        mBean.setMetricValue(metricData.metric, metricData.unit, value, type);
    }

    private void assertDoubleRendering(MetricDescriptor originalDescriptor, MetricData metricData, Number newValue) {
            assert !metricData.wasPresent
                    : "metric '" + originalDescriptor.toString()
                            + "' was rendered twice. Present value: " + metricValue(metricData)
                            + ", new value: " + newValue;
    }

    private Number metricValue(MetricData metricData) {
        try {
            return (Number) mBeans.get(metricData.objectName).getAttribute(metricData.metric);
        } catch (AttributeNotFoundException ex) {
            throw new IllegalStateException("Metric is marked as present but no mBean is registered with object name "
                    + "'" + metricData.objectName + "' and attribute '" + metricData.metric + "'");
        }
    }

    private MetricDescriptor copy(MetricDescriptor descriptor) {
        return DEFAULT_DESCRIPTOR_SUPPLIER.get().copy(descriptor);
    }

    private void unregisterMBeanIgnoreError(ObjectName objectName) {
        try {
            platformMBeanServer.unregisterMBean(objectName);
        } catch (InstanceNotFoundException ignored) {
            // Unregistration can by racy, we unregister from multiple places.
        } catch (MBeanRegistrationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void whenComplete() {
        // remove metrics that weren't present in current rendering
        for (Iterator<MetricData> iterator = metricNameToMetricData.values().iterator(); iterator.hasNext(); ) {
            MetricData metricData = iterator.next();
            if (!metricData.wasPresent) {
                iterator.remove();
                // remove the metric from the bean
                MetricsMBean mBean = mBeans.get(metricData.objectName);
                mBean.removeMetric(metricData.metric);
                // remove entire bean if no metric is left
                if (mBean.numAttributes() == 0) {
                    mBeans.remove(metricData.objectName);
                    unregisterMBeanIgnoreError(metricData.objectName);
                }
            } else {
                metricData.wasPresent = false;
            }
        }
    }

    // package-visible for test
    @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
    static String escapeObjectNameValue(String name) {
        if (!shouldEscapeObjectNameValue(name)) {
            return name;
        }

        int length = name.length();
        StringBuilder builder = new StringBuilder(length + (1 << 3));
        builder.append('"');
        for (int i = 0; i < length; i++) {
            char ch = name.charAt(i);
            if (ch == '\\' || ch == '"' || ch == '*' || ch == '?') {
                builder.append('\\');
            }
            if (ch == '\n') {
                builder.append("\\n");
            } else {
                builder.append(ch);
            }
        }
        builder.append('"');
        return builder.toString();
    }

    @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
    private static boolean shouldEscapeObjectNameValue(String name) {
        for (int i = 0; i < name.length(); i++) {
            char ch = name.charAt(i);
            if (ch == '=' || ch == ':' || ch == '*' || ch == '?' || ch == '"' || ch == '\n') {
                return true;
            }
        }
        return false;
    }

    private static class MetricData {
        public static final int INITIAL_MBEAN_BUILDER_CAPACITY = 250;
        public static final int INITIAL_MODULE_BUILDER_CAPACITY = 350;
        ObjectName objectName;
        String metric;
        String unit;
        boolean wasPresent;

        /**
         * See {@link com.hazelcast.config.MetricsJmxConfig#setEnabled(boolean)}.
         */
        @SuppressWarnings({"checkstyle:ExecutableStatementCount", "checkstyle:NPathComplexity",
                           "checkstyle:CyclomaticComplexity"})
        MetricData(MetricDescriptor descriptor, String instanceNameEscaped, String domainPrefix) {
            StringBuilder mBeanTags = new StringBuilder(INITIAL_MBEAN_BUILDER_CAPACITY);
            StringBuilder moduleBuilder = new StringBuilder(INITIAL_MODULE_BUILDER_CAPACITY);
            String module = null;
            metric = descriptor.metric();
            ProbeUnit descriptorUnit = descriptor.unit();
            unit = descriptorUnit != null ? descriptorUnit.name() : "unknown";
            MutableInteger tagCnt = new MutableInteger();
            for (int i = 0; i < descriptor.tagCount(); i++) {
                String tag = descriptor.tag(i);
                String tagValue = descriptor.tagValue(i);

                if ("module".equals(tag)) {
                    moduleBuilder.append(tagValue);
                } else {
                    if (mBeanTags.length() > 0) {
                        mBeanTags.append(',');
                    }
                    int newTagIdx = tagCnt.getAndInc();
                    mBeanTags.append("tag").append(newTagIdx).append('=')
                             .append(escapeObjectNameValue(tag + "=" + tagValue));
                }
            }
            if (moduleBuilder.length() > 0) {
                module = moduleBuilder.toString();
            }
            String discriminator = descriptor.discriminator();
            String discriminatorValue = descriptor.discriminatorValue();
            if (discriminator != null && discriminatorValue != null) {
                if (mBeanTags.length() > 0) {
                    mBeanTags.append(',');
                }
                int newTagIdx = tagCnt.getAndInc();
                mBeanTags.append("tag").append(newTagIdx).append("=")
                         .append(escapeObjectNameValue(discriminator + "=" + discriminatorValue));
            }

            assert metric != null : "metric == null";

            StringBuilder objectNameStr = new StringBuilder(mBeanTags.length() + (1 << 3 << 3));
            objectNameStr.append(domainPrefix);
            if (module != null) {
                objectNameStr.append('.').append(module);
            }
            objectNameStr.append(":type=Metrics");
            objectNameStr.append(",instance=").append(instanceNameEscaped);
            if (descriptor.prefix() != null) {
                objectNameStr.append(",prefix=").append(escapeObjectNameValue(descriptor.prefix()));
            }
            if (mBeanTags.length() > 0) {
                objectNameStr.append(',').append(mBeanTags);
            }
            try {
                // We use the ObjectName(String) constructor, not the one with Hashtable, because
                // this one preserves the tag order which is important for the tree structure in UI tools.
                objectName = new ObjectName(objectNameStr.toString());
            } catch (MalformedObjectNameException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void shutdown() {
        isShutdown = true;
        if (platformMBeanServer == null) {
            return;
        }
        try {
            // unregister the MBeans registered by this JmxPublisher
            // the mBeans map can't be used since it is not thread-safe
            // and is meant to be used by the publisher thread only
            ObjectName name = new ObjectName(domainPrefix + "*" + ":instance=" + instanceNameEscaped + ",type=Metrics,*");
            for (ObjectName bean : platformMBeanServer.queryNames(name, null)) {
                unregisterMBeanIgnoreError(bean);
            }
        } catch (MalformedObjectNameException e) {
            throw new RuntimeException("Exception when unregistering JMX beans", e);
        }
    }
}
