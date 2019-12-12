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

import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

/**
 * Helper class which checks whether JMX beans of the given member were de-registered.
 */
public final class JmxLeakHelper {

    private JmxLeakHelper() {
        // No-op.
    }

    public static void checkJmxBeans() {
        Collection<String> activeJmxBeans = getActiveJmxBeans();

        if (!activeJmxBeans.isEmpty()) {
            StringBuilder errorMessage = new StringBuilder("Unregistered JMX beans:\n");

            for (String activeJmxBean : activeJmxBeans) {
                errorMessage.append("\t").append(activeJmxBean).append("\n");
            }

            throw new IllegalStateException(errorMessage.toString());
        }
    }

    private static Collection<String> getActiveJmxBeans() {
        try {
            TreeSet<String> res = new TreeSet<>();

            ObjectName objectName = new ObjectName("com.hazelcast*:*");

            Set<ObjectInstance> instances = ManagementFactory.getPlatformMBeanServer().queryMBeans(objectName, null);

            for (ObjectInstance instance : instances) {
                String name = instance.getObjectName().getCanonicalName();

                if (name != null && name.startsWith("com.hazelcast")) {
                    res.add(name);
                }
            }

            return res;
        } catch (Exception e) {
            throw new RuntimeException("Failed to check active JMX beans: " + e.getMessage(), e);
        }
    }
}
