package com.hazelcast.jmx;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.lang.management.ManagementFactory;
import java.util.Hashtable;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;

import static org.junit.Assert.fail;

/**
 * Holds the mbean server and hazelcast and provides some utility functions for accessing the mbeans.
 */
public final class JmxTestDataHolder {
    private HazelcastInstance hz;
    private MBeanServer mbs;

    /**
     * Initialize with new hazelcast instance and mbean server
     */
    public JmxTestDataHolder(TestHazelcastInstanceFactory factory) {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_ENABLE_JMX, "true");
        hz = factory.newHazelcastInstance(config);
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
    public Object getMBeanAttribute(final String type, final String objectName, String attributeName)
            throws MalformedObjectNameException, AttributeNotFoundException, MBeanException, ReflectionException, InstanceNotFoundException {
        return mbs.getAttribute(getObjectName(type, objectName), attributeName);
    }

    /**
     * Calls the mbean operation with given attributes
     *
     * @param type          Type of the Hazelcast object (first level of hierarchy), e.g. "IMap"
     * @param objectName    Name of the Hazelcast object (second level of hierarchy), e.g. "myMap"
     * @param operationName Name of attribute to query, e.g. "size"
     * @param params        Parameters for call. May be null for methods without parameters.
     * @param signature     Class names of parameters, see {@link javax.management.MBeanServerConnection#invoke(javax.management.ObjectName, String, Object[], String[])}
     *                      May be null for methods without parameters.
     * @return Value to get
     */
    public Object invokeMBeanOperation(final String type, final String objectName, String operationName, Object[] params, String[] signature)
            throws MalformedObjectNameException, AttributeNotFoundException, MBeanException, ReflectionException, InstanceNotFoundException {
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
