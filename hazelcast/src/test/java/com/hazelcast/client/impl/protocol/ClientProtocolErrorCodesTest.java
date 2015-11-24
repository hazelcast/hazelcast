package com.hazelcast.client.impl.protocol;

import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

public class ClientProtocolErrorCodesTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ClientProtocolErrorCodes.class);
    }
}
