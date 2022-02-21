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

package com.hazelcast.test;

import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Helper class which checks whether JMX beans of the given member were de-registered.
 */
public final class JmxLeakHelper {
    /** A flag to ignore the check one time. */
    private static boolean ignoreOnce;

    private JmxLeakHelper() {
        // No-op.
    }

    public static Collection<ObjectInstance> getBeansByClass(Class clazz) {
        String className = clazz.getName();

        List<ObjectInstance> res = new ArrayList<>();

        for (ObjectInstance activeJmxBean : getActiveJmxBeans()) {
            if (className.equals(activeJmxBean.getClassName())) {
                res.add(activeJmxBean);
            }
        }

        return res;
    }

    public static void checkJmxBeans() {
        //noinspection ConstantConditions
        if (true) {
            // TODO: Disabled due to racy JMX registration (see https://github.com/hazelcast/hazelcast/issues/16384).
            //  Please re-enable when fixed.
            return;
        }

        if (ignoreOnce) {
            ignoreOnce = false;

            return;
        }

        Collection<String> activeJmxBeanNames = getActiveJmxBeanNames();

        if (!activeJmxBeanNames.isEmpty()) {
            StringBuilder errorMessage = new StringBuilder("JMX beans are still registered:\n");

            for (String activeJmxBeanName : activeJmxBeanNames) {
                errorMessage.append("\t").append(activeJmxBeanName).append("\n");
            }

            throw new IllegalStateException(errorMessage.toString());
        }
    }

    private static Collection<String> getActiveJmxBeanNames() {
        Collection<ObjectInstance> activeJmxBeans = getActiveJmxBeans();

        Set<String> res = new TreeSet<>();

        for (ObjectInstance activeJmxBean : activeJmxBeans) {
            res.add(activeJmxBean.getObjectName().getCanonicalName());
        }

        return res;
    }

    public static Collection<ObjectInstance> getActiveJmxBeansWithPrefix(String prefix) {
        return getActiveJmxBeans(prefix);
    }

    private static Collection<ObjectInstance> getActiveJmxBeans() {
        return getActiveJmxBeans("com.hazelcast");
    }

    private static Collection<ObjectInstance> getActiveJmxBeans(String prefix) {
        try {
            List<ObjectInstance> res = new ArrayList<>();

            ObjectName objectName = new ObjectName(prefix + "*:*");

            Set<ObjectInstance> instances = ManagementFactory.getPlatformMBeanServer().queryMBeans(objectName, null);

            for (ObjectInstance instance : instances) {
                String name = instance.getObjectName().getCanonicalName();

                if (name != null && name.startsWith(prefix)) {
                    res.add(instance);
                }
            }

            return res;
        } catch (Exception e) {
            throw new RuntimeException("Failed to check active JMX beans: " + e.getMessage(), e);
        }
    }

    public static void ignoreOnce() {
        ignoreOnce = true;
    }
}
