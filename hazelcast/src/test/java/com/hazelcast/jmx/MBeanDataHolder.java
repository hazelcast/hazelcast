package com.hazelcast.jmx;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Hashtable;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Holds the Hazelcast instance and MBean server and provides some utility functions for accessing the MBeans.
 */
final class MBeanDataHolder {

    private HazelcastInstance hz;
    private MBeanServer mbs;

    /**
     * Initialize with new hazelcast instance and MBean server
     */
    MBeanDataHolder(TestHazelcastInstanceFactory factory) {
        Config config = new Config();
        config.setProperty(GroupProperty.ENABLE_JMX, "true");
        hz = factory.newHazelcastInstance(config);
        mbs = ManagementFactory.getPlatformMBeanServer();
    }

    void assertMBeanExistEventually(final String type, final String name) {
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

    void assertMBeanNotExistEventually(final String type, final String name) {
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
    Object getMBeanAttribute(final String type, final String objectName, String attributeName) throws Exception {
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
    Object invokeMBeanOperation(final String type, final String objectName, String operationName, Object[] params,
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
        table.put("type", type);
        table.put("name", objectName);
        table.put("instance", hz.getName());
        return new ObjectName("com.hazelcast", table);
    }
}
