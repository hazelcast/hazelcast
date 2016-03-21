package com.hazelcast.internal.properties;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GroupPropertyTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() throws Exception {
        assertUtilityConstructor(GroupProperty.class);
    }

    @Test
    public void testGetPropertyFromHazelcastInstance() {
        String propertyKey = GroupProperty.ENTERPRISE_LICENSE_KEY.getName();

        Config config = new Config();
        config.setProperty(propertyKey, "notDefaultValue");
        HazelcastInstance instance = createHazelcastInstance(config);

        assertEquals("notDefaultValue", instance.getProperty(propertyKey));
    }
}
