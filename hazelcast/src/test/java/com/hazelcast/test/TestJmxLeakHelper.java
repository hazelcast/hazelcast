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

package com.hazelcast.test;

import com.hazelcast.internal.metrics.jmx.JmxPublisher;

import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helper class which checks whether JMX beans of the given member were de-registered.
 */
public final class TestJmxLeakHelper {

    private TestJmxLeakHelper() {
        // No-op.
    }

    public static void checkJmxBeans(String instanceName) {
        checkJmxBeans(Collections.singleton(instanceName));
    }

    public static void checkJmxBeans(Collection<String> instanceNames) {
        Map<String, List<String>> problems = new HashMap<>();

        for (String instanceName : instanceNames) {
            List<String> activeJmxBeans = getActiveJmxBeans(instanceName);

            if (!activeJmxBeans.isEmpty()) {
                problems.put(instanceName, activeJmxBeans);
            }
        }

        if (!problems.isEmpty()) {
            StringBuilder errorMessage = new StringBuilder("Failed to unregister JMX bean for some members:\n");

            for (Map.Entry<String, List<String>> entry : problems.entrySet()) {
                String instanceName = entry.getKey();
                List<String> activeJmxBeans = entry.getValue();

                errorMessage.append("\t").append(instanceName);

                for (String activeJmxBean : activeJmxBeans) {
                    errorMessage.append("\t\t").append(activeJmxBean);
                }
            }

            throw new IllegalStateException(errorMessage.toString());
        }
    }

    private static List<String> getActiveJmxBeans(String instanceName) {
        // Escape instance name the same way it is escaped for member JMX service.
        String instanceNameEscaped = JmxPublisher.escapeObjectNameValue(instanceName);

        try {
            List<String> res = new ArrayList<>();

            ObjectName objectName = new ObjectName("com.hazelcast*:*");

            Set<ObjectInstance> instances = ManagementFactory.getPlatformMBeanServer().queryMBeans(objectName, null);

            for (ObjectInstance instance : instances) {
                String name = instance.getObjectName().getCanonicalName();

                if (name != null && name.startsWith("com.hazelcast:instance=" + instanceNameEscaped + ",")) {
                    res.add(name);
                }
            }

            return res;
        } catch (Exception e) {
            throw new RuntimeException("Failed to check active JMX beans: " + e.getMessage(), e);
        }
    }
}
