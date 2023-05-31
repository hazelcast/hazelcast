/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Formatter;
import java.util.Set;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.core.JetTestSupport.getJetServiceBackend;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

class JmxMetricsChecker {

    private static final String PREFIX = "com.hazelcast.jet";

    private final MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();

    private final ObjectName descriptor;

    private JmxMetricsChecker(ObjectName descriptor) {
        this.descriptor = descriptor;
    }

    static JmxMetricsChecker forInstance(HazelcastInstance instance) throws Exception {
        return new JmxMetricsChecker(new ObjectNameBuilder(instance.getName()).build());
    }

    static JmxMetricsChecker forJob(HazelcastInstance instance, Job job) throws Exception {
        return new JmxMetricsChecker(getJobDescriptor(instance, job).build());
    }

    static JmxMetricsChecker forExecution(HazelcastInstance instance, Job job, String... extraTags) throws Exception {
        ObjectNameBuilder builder = getJobDescriptor(instance, job);
        long executionId = getJetServiceBackend(instance).getJobExecutionService()
                                                         .getExecutionIdForJobId(job.getId());
        builder.tag(MetricTags.EXECUTION, idToString(executionId));
        for (String extraTag : extraTags) {
            builder.tag(extraTag);
        }
        return new JmxMetricsChecker(builder.build());
    }

    private static ObjectNameBuilder getJobDescriptor(HazelcastInstance instance, Job job) {
        ObjectNameBuilder builder = new ObjectNameBuilder(instance.getName());
        String jobId = job.getIdString();
        return builder.tag(MetricTags.JOB, jobId)
                      .tag(MetricTags.JOB_NAME, job.getName() != null ? job.getName() : jobId);
    }

    long getMetricValue(String metricName) throws Exception {
        Set<ObjectName> publishedDescriptors = platformMBeanServer
                .queryMBeans(new ObjectName(PREFIX + ":*"), null)
                .stream().map(ObjectInstance::getObjectName).collect(toSet());
        assertTrue("No metric is published for '" + descriptor + "'",
                publishedDescriptors.contains(descriptor));
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

    static class ObjectNameBuilder {
        final Formatter f = new Formatter();
        int tagCount = 0;

        ObjectNameBuilder(String instance) {
            f.format("%s:type=Metrics,instance=%s", PREFIX, instance);
        }

        ObjectNameBuilder tag(String name, String value) {
            f.format(",tag%d=\"%s=%s\"", tagCount++, name, value);
            return this;
        }

        ObjectNameBuilder tag(String tag) {
            f.format(",tag%d=\"%s\"", tagCount++, tag);
            return this;
        }

        public ObjectName build() throws MalformedObjectNameException {
            return new ObjectName(f.toString());
        }
    }
}
