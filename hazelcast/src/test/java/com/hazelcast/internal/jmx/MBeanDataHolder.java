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

package com.hazelcast.internal.jmx;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.jmx.ManagementService.quote;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Holds the Hazelcast instance and MBean server and provides some utility functions for accessing the MBeans.
 */
public final class MBeanDataHolder {

    private static final AtomicInteger ID_GEN = new AtomicInteger(0);

    private HazelcastInstance hz;
    private MBeanServer mbs;

    /**
     * Initialize with new hazelcast instance and MBean server
     */
    public MBeanDataHolder(TestHazelcastInstanceFactory factory) {
       this(factory, new Config());
    }

    public MBeanDataHolder(TestHazelcastInstanceFactory factory, Config config) {
        config.setInstanceName("hz:\",=*?" + ID_GEN.getAndIncrement());
        config.setProperty(ClusterProperty.ENABLE_JMX.getName(), "true");
        hz = factory.newHazelcastInstance(config);
        mbs = ManagementFactory.getPlatformMBeanServer();
    }

    public MBeanDataHolder(HazelcastInstance instance) {
        hz = instance;
        mbs = ManagementFactory.getPlatformMBeanServer();
    }

    public void assertMBeanExistEventually(final String type, final String name) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                ObjectName object = getObjectName(type, name);
                try {
                    mbs.getObjectInstance(object);
                } catch (InstanceNotFoundException e) {
                    fail(e.getMessage());
                }
            }
        });
    }

    public void assertMBeanNotExistEventually(final String type, final String name) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                ObjectName object = getObjectName(type, name);
                assertFalse(mbs.isRegistered(object));
            }
        });
    }

    /**
     * @return Hazelcast instance managed by this holder
     */
    public HazelcastInstance getHz() {
        return hz;
    }

    /**
     * @return MBean server managed by this holder
     */
    public MBeanServer getMbs() {
        return mbs;
    }

    /**
     * Get the value of the JMX attribute with the specified name
     *
     * @param type          Type of the Hazelcast object (first level of hierarchy), e.g. "IMap"
     * @param objectName    Name of the Hazelcast object (second level of hierarchy), e.g. "myMap"
     * @param attributeName Name of attribute to query, e.g. "size"
     * @return Value to get
     */
    public Object getMBeanAttribute(final String type, final String objectName, String attributeName) throws Exception {
        return mbs.getAttribute(getObjectName(type, objectName), attributeName);
    }

    /**
     * Calls the MBean operation with given attributes
     *
     * @param type          Type of the Hazelcast object (first level of hierarchy), e.g. "IMap"
     * @param objectName    Name of the Hazelcast object (second level of hierarchy), e.g. "myMap"
     * @param operationName Name of attribute to query, e.g. "size"
     * @param params        Parameters for call. May be null for methods without parameters.
     * @param signature     Class names of parameters, see {@link javax.management.MBeanServerConnection#invoke(javax.management.ObjectName, String, Object[], String[])}
     *                      May be null for methods without parameters.
     * @return Value to get
     */
    public Object invokeMBeanOperation(final String type, final String objectName, String operationName, Object[] params,
                                       String[] signature) throws Exception {
        return mbs.invoke(getObjectName(type, objectName), operationName, params, signature);
    }

    /**
     * JMX object name
     *
     * @param type       Type of the Hazelcast object (first level of hierarchy), e.g. "IMap"
     * @param objectName Name of the Hazelcast object (second level of hierarchy), e.g. "myMap"
     * @return JMX object name
     */
    private ObjectName getObjectName(String type, String objectName) throws MalformedObjectNameException {
        Hashtable<String, String> table = new Hashtable<String, String>();
        table.put("type", quote(type));
        table.put("name", quote(objectName));
        table.put("instance", quote(hz.getName()));
        return new ObjectName("com.hazelcast", table);
    }
}
