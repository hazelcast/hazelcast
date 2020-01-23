/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.util.BiTuple;

import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JmxPublisherTestHelper {
    private static final String MODULE_NAME = "moduleA";

    private final MBeanServer platformMBeanServer;
    private final ObjectName objectNameNoModule;
    private final ObjectName objectNameWithModule;

    public JmxPublisherTestHelper(String domainPrefix) throws Exception {
        objectNameNoModule = new ObjectName(domainPrefix + ":*");
        objectNameWithModule = new ObjectName(domainPrefix + "." + MODULE_NAME + ":*");
        platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    public void assertNoMBeans() {
        Set<ObjectInstance> instances = queryOurInstances();
        assertEquals(0, instances.size());
    }

    public void assertMBeanContains(String... expectedObjectNames) throws Exception {
        Set<ObjectInstance> instances = queryOurInstances();
        Map<ObjectName, ObjectInstance> instanceMap =
                instances.stream().collect(Collectors.toMap(ObjectInstance::getObjectName, Function.identity()));

        for (String expectedObjectName : expectedObjectNames) {
            ObjectName on = new ObjectName(expectedObjectName);
            assertTrue("name: " + on + " not in instances " + instances, instanceMap.containsKey(on));
        }
    }

    public void assertMBeans(List<BiTuple<String, List<Map.Entry<String, Number>>>> expected) throws Exception {
        Set<ObjectInstance> instances = queryOurInstances();
        Map<ObjectName, ObjectInstance> instanceMap =
                instances.stream().collect(Collectors.toMap(ObjectInstance::getObjectName, Function.identity()));

        assertEquals("actual: " + instances, expected.size(), instances.size());

        for (BiTuple<String, List<Map.Entry<String, Number>>> entry : expected) {
            ObjectName on = new ObjectName(entry.element1);
            assertTrue("name: " + on + " not in instances " + instances, instanceMap.containsKey(on));
            for (Map.Entry<String, Number> attribute : entry.element2) {
                assertEquals("Attribute '" + attribute.getKey() + "' of '" + on + "' doesn't match",
                        attribute.getValue(), platformMBeanServer.getAttribute(on, attribute.getKey()));
            }
        }
    }

    public Set<ObjectInstance> queryOurInstances() {
        Set<ObjectInstance> instances = new HashSet<>();
        instances.addAll(platformMBeanServer.queryMBeans(objectNameNoModule, null));
        instances.addAll(platformMBeanServer.queryMBeans(objectNameWithModule, null));
        return instances;
    }
}
