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

package com.hazelcast.jet.impl.metrics.jmx;

import com.hazelcast.internal.metrics.MetricsUtil;
import com.hazelcast.jet.config.MetricsConfig;
import com.hazelcast.jet.impl.metrics.MetricsPublisher;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;

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
    private final Map<String, MetricData> metricNameToMetricData = new HashMap<>();
    /**
     * key: jmx object name, value: mBean
     */
    private final Map<ObjectName, MetricsMBean> mBeans = new HashMap<>();
    private final String domainPrefix;

    public JmxPublisher(String instanceName, String domainPrefix) {
        this.domainPrefix = domainPrefix;
        platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        instanceNameEscaped = escapeObjectNameValue(instanceName);
    }

    @Override
    public String name() {
        return "JMX Publisher";
    }

    @Override
    public void publishLong(String metricName, long value) {
        renderNumber(metricName, value);
    }

    @Override
    public void publishDouble(String metricName, double value) {
        renderNumber(metricName, value);
    }

    private void renderNumber(String metricName, Number value) {
        MetricData metricData = metricNameToMetricData.computeIfAbsent(metricName,
                n -> new MetricData(n, instanceNameEscaped, domainPrefix));
        assert !metricData.wasPresent : "metric '" + metricName + "' was rendered twice";
        metricData.wasPresent = true;
        MetricsMBean mBean = mBeans.computeIfAbsent(metricData.objectName, objectName -> {
            MetricsMBean bean = new MetricsMBean();
            try {
                platformMBeanServer.registerMBean(bean, objectName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return bean;
        });
        mBean.setMetricValue(metricData.metric, metricData.unit, value);
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
                    try {
                        platformMBeanServer.unregisterMBean(metricData.objectName);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            } else {
                metricData.wasPresent = false;
            }
        }
    }

    @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
    private static String escapeObjectNameValue(String name) {
        if (name.indexOf(',') < 0
                && name.indexOf('=') < 0
                && name.indexOf(':') < 0
                && name.indexOf('*') < 0
                && name.indexOf('?') < 0
                && name.indexOf('\"') < 0
                && name.indexOf('\n') < 0) {
            return name;
        }
        return "\"" + name.replace("\"", "\\\"").replace("\n", "\\n ") + '"';
    }

    private static class MetricData {
        ObjectName objectName;
        String metric;
        String unit;
        boolean wasPresent;

        /**
         * See {@link
         * MetricsConfig#setJmxEnabled(boolean)}.
         */
        @SuppressWarnings("checkstyle:ExecutableStatementCount")
        MetricData(String metricName, String instanceNameEscaped, String domainPrefix) {
            List<Entry<String, String>> tagsList = metricName.startsWith("[") && metricName.endsWith("]")
                    ? MetricsUtil.parseMetricName(metricName)
                    : parseOldMetricName(metricName);
            StringBuilder mBeanTags = new StringBuilder();
            String module = null;
            metric = "metric";
            unit = "unknown";
            int tag = 0;

            for (Entry<String, String> entry : tagsList) {
                switch (entry.getKey()) {
                    case "unit":
                        unit = entry.getValue();
                        break;
                    case "module":
                        module = entry.getValue();
                        break;
                    case "metric":
                        metric = entry.getValue();
                        break;
                    default:
                        if (mBeanTags.length() > 0) {
                            mBeanTags.append(',');
                        }
                        mBeanTags.append("tag")
                                .append(tag++)
                                .append('=');
                        if (entry.getKey().length() == 0) {
                            // key is empty for old metric names (see parseOldMetricName)
                            mBeanTags.append(escapeObjectNameValue(entry.getValue()));
                        } else {
                            // We add both key and value to the value: the key carries semantic value, we
                            // want to see it in the UI.
                            mBeanTags.append(escapeObjectNameValue(entry.getKey() + '=' + entry.getValue()));
                        }
                }
            }
            assert metric != null : "metric == null";

            StringBuilder objectNameStr = new StringBuilder(mBeanTags.length() + (1 << 3 << 3));
            objectNameStr.append(domainPrefix);
            if (module != null) {
                objectNameStr.append('.').append(module);
            }
            objectNameStr.append(":type=Metrics");
            objectNameStr.append(",instance=").append(instanceNameEscaped);
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

        private static List<Entry<String, String>> parseOldMetricName(String name) {
            List<Entry<String, String>> res = new ArrayList<>();
            boolean inBracket = false;
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < name.length(); i++) {
                char ch = name.charAt(i);
                if (ch == '.' && !inBracket) {
                    if (builder.length() > 0) {
                        res.add(entry("", builder.toString()));
                    }
                    builder.setLength(0);
                } else {
                    builder.append(ch);
                    if (ch == '[') {
                        inBracket = true;
                    } else if (ch == ']') {
                        inBracket = false;
                    }
                }
            }
            // the trailing entry will be marked as `metric` tag
            if (builder.length() > 0) {
                res.add(entry("metric", builder.toString()));
            }
            return res;
        }
    }

    @Override
    public void shutdown() {
        try {
            ObjectName name = new ObjectName("com.hazelcast*" + ":instance=" + instanceNameEscaped + ",type=Metrics,*");
            for (ObjectName bean : platformMBeanServer.queryNames(name, null)) {
                platformMBeanServer.unregisterMBean(bean);
            }
        } catch (Exception e) {
            throw new RuntimeException("Exception when unregistering JMX beans", e);
        }
    }
}
