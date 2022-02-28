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

package com.hazelcast.internal.metrics.jmx;

import com.hazelcast.internal.metrics.jmx.MetricsMBean.Type;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.TriTuple;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.management.MBeanAttributeInfo;
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
    private final ILogger logger = Logger.getLogger(JmxPublisherTestHelper.class);

    public JmxPublisherTestHelper(String domainPrefix) throws Exception {
        objectNameNoModule = new ObjectName(domainPrefix + ":*");
        objectNameWithModule = new ObjectName(domainPrefix + "." + MODULE_NAME + ":*");
        platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    public void clearMBeans() {
        queryOurInstances().forEach(this::clearMBean);
        assertNoMBeans();
    }

    private void clearMBean(ObjectInstance instance) {
        try {
            ObjectName name = instance.getObjectName();
            platformMBeanServer.unregisterMBean(name);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public void assertNoMBeans() {
        Set<ObjectInstance> instances = queryOurInstances();
        if (instances.size() > 0) {
            String jvmName = ManagementFactory.getRuntimeMXBean().getName();
            logger.info("Dangling metrics MBeans created by " + jvmName + ": " + instances);
        }
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

    public void assertMBeans(List<BiTuple<String, List<TriTuple<String, Number, Type>>>> expected) throws Exception {
        Set<ObjectInstance> instances = queryOurInstances();
        Map<ObjectName, ObjectInstance> instanceMap =
                instances.stream().collect(Collectors.toMap(ObjectInstance::getObjectName, Function.identity()));

        assertEquals("actual: " + instances, expected.size(), instances.size());

        for (BiTuple<String, List<TriTuple<String, Number, Type>>> entry : expected) {
            ObjectName on = new ObjectName(entry.element1);
            assertTrue("name: " + on + " not in instances " + instances, instanceMap.containsKey(on));
            for (TriTuple<String, Number, Type> attribute : entry.element2) {
                assertEquals("Attribute '" + attribute.element1 + "' of '" + on + "' doesn't match",
                        attribute.element2, platformMBeanServer.getAttribute(on, attribute.element1));
                MBeanAttributeInfo[] mbeanInfos = platformMBeanServer.getMBeanInfo(on).getAttributes();
                boolean match = false;
                for (int i = 0; i < mbeanInfos.length && !match; i++) {
                    if (attribute.element1.equals(mbeanInfos[i].getName())) {
                        assertEquals("Attribute type '" + attribute.element1 + "' of '" + on + "' doesn't match",
                                attribute.element3, Type.of(mbeanInfos[i].getType()));
                        match = true;
                    }
                }
                assertTrue("No matching MBean attribute type for checking the type of '" + on + "'", match);
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
