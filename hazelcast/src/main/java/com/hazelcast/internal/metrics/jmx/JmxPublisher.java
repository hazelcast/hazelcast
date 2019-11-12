/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.MetricsConfig;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsPublisher;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.util.MutableInteger;

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

/**
 * Renderer to create, register and unregister mBeans for metrics as they are
 * rendered.
 */
public class JmxPublisher implements MetricsPublisher {

    private final MBeanServer platformMBeanServer;
    private final String instanceNameEscaped;

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
        publishNumber(descriptor, value);
    }

    @Override
    public void publishDouble(MetricDescriptor descriptor, double value) {
        publishNumber(descriptor, value);
    }

    private void publishNumber(MetricDescriptor originalDescriptor, Number value) {
        if (originalDescriptor.isTargetExcluded(JMX)) {
            return;
        }

        // we need to take a copy of originalDescriptor here to ensure
        // we map with an instance that doesn't get recycled or mutated
        MetricDescriptor descriptor = copy(originalDescriptor);
        MetricData metricData = metricNameToMetricData.computeIfAbsent(descriptor, createMetricDataFunction);
        assert !metricData.wasPresent : "metric '" + originalDescriptor.toString() + "' was rendered twice";
        metricData.wasPresent = true;
        MetricsMBean mBean = mBeans.computeIfAbsent(metricData.objectName, createMBeanFunction);
        if (isShutdown) {
            unregisterMBeanIgnoreError(metricData.objectName);
        }
        mBean.setMetricValue(metricData.metric, metricData.unit, value);
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
        if (name.indexOf(',') < 0
                && name.indexOf('=') < 0
                && name.indexOf(':') < 0
                && name.indexOf('*') < 0
                && name.indexOf('?') < 0
                && name.indexOf('\"') < 0
                && name.indexOf('\n') < 0) {
            return name;
        }
        return "\"" + name
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("*", "\\*")
                .replace("?", "\\?")
                + '"';
    }

    private static class MetricData {
        ObjectName objectName;
        String metric;
        String unit;
        boolean wasPresent;

        /**
         * See {@link MetricsConfig#setJmxEnabled(boolean)}.
         */
        @SuppressWarnings({"checkstyle:ExecutableStatementCount", "checkstyle:NPathComplexity",
                           "checkstyle:CyclomaticComplexity"})
        MetricData(MetricDescriptor descriptor, String instanceNameEscaped, String domainPrefix) {
            StringBuilder mBeanTags = new StringBuilder();
            StringBuilder moduleBuilder = new StringBuilder();
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
        ObjectName name;
        try {
            name = new ObjectName("com.hazelcast*" + ":instance=" + instanceNameEscaped + ",type=Metrics,*");
        } catch (MalformedObjectNameException e) {
            throw new RuntimeException("Exception when unregistering JMX beans", e);
        }

        for (ObjectName bean : platformMBeanServer.queryNames(name, null)) {
            unregisterMBeanIgnoreError(bean);
        }
    }
}
