package com.hazelcast.internal.properties;

import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

public class GroupPropertyTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() throws Exception {
        assertUtilityConstructor(GroupProperty.class);
    }
}
