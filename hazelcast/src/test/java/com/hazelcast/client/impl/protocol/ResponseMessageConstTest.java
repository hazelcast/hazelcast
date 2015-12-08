package com.hazelcast.client.impl.protocol;

import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

public class ResponseMessageConstTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() throws Exception {
        assertUtilityConstructor(ResponseMessageConst.class);
    }
}
