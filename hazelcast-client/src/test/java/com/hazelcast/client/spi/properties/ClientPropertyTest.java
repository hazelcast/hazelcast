package com.hazelcast.client.spi.properties;

import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

public class ClientPropertyTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() throws Exception {
        assertUtilityConstructor(ClientProperty.class);
    }
}
