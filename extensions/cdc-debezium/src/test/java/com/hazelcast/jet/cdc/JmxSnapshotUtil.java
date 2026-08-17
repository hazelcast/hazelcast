/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.cdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static java.time.temporal.ChronoUnit.MINUTES;
import static org.junit.Assert.fail;

public final class JmxSnapshotUtil {
    private static final Logger LOG = LoggerFactory.getLogger(JmxSnapshotUtil.class);

    private JmxSnapshotUtil() {
    }

    public static void waitForSnapshotEnd(String jmxDbType, String server) {
        sleep(1);
        String basicObjectName = "debezium." + jmxDbType + ":type=connector-metrics,context=snapshot,server=" + server;
        String objectNameAlt = basicObjectName + ",task=0";
        List<String> possibleMetricNames = List.of(basicObjectName, objectNameAlt);

        Instant start = Instant.now();
        Instant maxEnd = start.plus(2, MINUTES);
        boolean finished = false;
        while (!finished) {
            Instant end = Instant.now();
            if (end.isAfter(maxEnd)) {
                if (LOG.isDebugEnabled()) {
                    LOG.error("JMX waiting failed, tried: {}", possibleMetricNames);
                    printJmx();
                }
                fail("Snapshot wait timed out");
            }
            try (var jmx = JMXConnectorFactory.connect(
                    new JMXServiceURL("service:jmx:rmi://%s:%s/jndi/rmi://%s:%s/jmxrmi"
                            .formatted("localhost", 1099, "localhost", 1099)), null)) {
                MBeanServerConnection connection = jmx.getMBeanServerConnection();
                for (String name : possibleMetricNames) {
                    if (snapshotCompleted(connection, name)) {
                        finished = true;
                        break;
                    }
                }
                sleep(1);
            } catch (Exception ignored) {
            }
        }
        sleep(2);
        LOG.info("Snapshot should have ended by now");
    }

    private static void sleep(int seconds) {
        try {
            Thread.sleep(seconds * 1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static boolean snapshotCompleted(MBeanServerConnection connection, String objectName) {
        try {
            Object rawValue = connection.getAttribute(new ObjectName(objectName), "SnapshotCompleted");
            return Boolean.parseBoolean(String.valueOf(rawValue));
        } catch (Exception ignored) {
            return false;
        }
    }

    static void printJmx() {
        try (var jmx = JMXConnectorFactory.connect(
                new JMXServiceURL("service:jmx:rmi://%s:%s/jndi/rmi://%s:%s/jmxrmi"
                        .formatted("localhost", 1099, "localhost", 1099)), null)) {

            MBeanServerConnection connection = jmx.getMBeanServerConnection();
            Set<ObjectName> objectNames = connection.queryNames(null, null);
            for (ObjectName name : objectNames) {
                System.out.println("Object name: " + name.getCanonicalName());
                var attributes = connection.getMBeanInfo(name).getAttributes();
                for (var attributeInfo : attributes) {
                    System.out.println("\t" + attributeInfo.getName()
                            + " -> " + connection.getAttribute(name, attributeInfo.getName()));
                }
                System.out.println("------ end of " + name.getCanonicalName());
            }
        } catch (Exception ignored) {
        }
    }
}
