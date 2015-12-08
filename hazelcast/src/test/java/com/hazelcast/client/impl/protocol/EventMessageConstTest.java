package com.hazelcast.client.impl.protocol;

import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

public class EventMessageConstTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() throws Exception {
        assertUtilityConstructor(EventMessageConst.class);
    }
}
