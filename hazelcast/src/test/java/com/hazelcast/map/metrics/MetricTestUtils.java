/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.metrics;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import java.lang.management.ManagementFactory;

import static org.assertj.core.api.Assertions.assertThat;

public class MetricTestUtils {

    private static final long RETRY_WAIT_MILLIS = 200;
    private static final String METRIC_NAME_PREFIX = "com.hazelcast:type=Metrics";
    private static final int MBEAN_ASSERTION_RETRIES = 20;

    private MetricTestUtils() {
    }

    /**
     * Set metrics collection frequency to 1 second on the supplied config and return it
     */
    static Config setRapidMetricsCollection(Config input) {
        input.getMetricsConfig().setCollectionFrequencySeconds(1);
        return input;
    }

    /**
     * Build the {@link ObjectName} pointing to the published JMX metrics object for an IMap index
     */
    static ObjectName buildMapIndexMetricName(HazelcastInstance instance, String mapName, String indexName) {
        try {
            return new ObjectName(
                    "%s,instance=%s,prefix=map.index,tag0=\"index=%s\",tag1=\"name=%s\"".formatted(METRIC_NAME_PREFIX,
                            instance.getName(), indexName, mapName));
        } catch (MalformedObjectNameException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Build the {@link ObjectName} pointing to the published JMX metrics object for an IMap
     */
    static ObjectName buildMapMetricName(HazelcastInstance instance, String mapName) {
        try {
            return new ObjectName(
                    "%s,instance=%s,prefix=map,tag0=\"name=%s\"".formatted(METRIC_NAME_PREFIX, instance.getName(), mapName));
        } catch (MalformedObjectNameException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Assert that the attribute value for an MBean in the local platform server equals the expected value
     * at least once in a series of query retries. We perform retries to handle the case where an updated
     * attribute value may not have been published yet.
     *
     * @param expected      The expected value
     * @param mbean         The MBean object name
     * @param attributeName The attribute name on the object we are asserting the value of
     */
    static void assertAttributeEquals(Object expected, ObjectName mbean, String attributeName) {
        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        for (int i = 0; i < MBEAN_ASSERTION_RETRIES; i++) {
            try {
                assertThat(mbeanServer.getAttribute(mbean, attributeName)).isEqualTo(expected);
            } catch (AssertionError | InstanceNotFoundException | AttributeNotFoundException e) {
                if (i == MBEAN_ASSERTION_RETRIES - 1) {
                    throw e instanceof AssertionError ? (AssertionError) e : new AssertionError(e);
                } else {
                    sleep();
                    continue;
                }
            } catch (MBeanException | ReflectionException e) {
                throw new RuntimeException(e);
            }
            return;
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(RETRY_WAIT_MILLIS);
        } catch (InterruptedException e) {
            throw new AssertionError("Test interrupted");
        }
    }
}
