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

package com.hazelcast.jet.core.metrics;

import com.hazelcast.jet.Job;

import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

class JmxMetricsChecker {

    private static final String PREFIX = "com.hazelcast.jet";

    private final MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();

    private final ObjectName descriptor;

    JmxMetricsChecker(String instance) throws Exception {
        this.descriptor = new ObjectName(getName(instance));
    }

    JmxMetricsChecker(String instance, Job job) throws Exception {
        this.descriptor = new ObjectName(getName(instance, job));
    }

    JmxMetricsChecker(String instance, Job job, String... extraTags) throws Exception {
        this.descriptor = new ObjectName(getName(instance, job, extraTags));
    }

    long getMetricValue(String metricName) throws Exception {
        Set<ObjectName> publishedDescriptors = platformMBeanServer
                .queryMBeans(new ObjectName(PREFIX + ":*"), null)
                .stream().map(ObjectInstance::getObjectName).collect(toSet());
        assertTrue("metric '" + metricName + "' not published", publishedDescriptors.contains(descriptor));
        return (long) platformMBeanServer.getAttribute(descriptor, metricName);
    }

    void assertMetricValue(String metricName, long expectedValue) throws Exception {
        long actualValue = getMetricValue(metricName);
        assertEquals(expectedValue, actualValue);
    }

    long assertMetricValueAtLeast(String metricName, long minExpectedValue) throws Exception {
        long actualValue = getMetricValue(metricName);
        assertTrue(actualValue >= minExpectedValue);
        return actualValue;
    }

    private static String getName(String instance, Job job, String... extraTags) {
        String jobId = job.getIdString();
        String execId = job.getMetrics().get(MetricNames.RECEIVED_COUNT).get(0).tag("exec");

        StringBuilder sb = new StringBuilder();
        sb.append(getName(instance, "job=" + jobId, "exec=" + execId));
        return appendTags(sb, 2, extraTags).toString();
    }

    private static String getName(String instance, String... extraTags) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("com.hazelcast.jet:type=Metrics,instance=%s", instance));
        return appendTags(sb, 0, extraTags).toString();
    }

    private static StringBuilder appendTags(StringBuilder sb, int offset, String... extraTags) {
        for (int i = 0; i < extraTags.length; i++) {
            sb.append(String.format(",tag%d=\"%s\"", offset + i, extraTags[i]));
        }
        return sb;
    }

}
