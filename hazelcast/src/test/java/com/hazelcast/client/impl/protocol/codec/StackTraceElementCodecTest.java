package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

public class StackTraceElementCodecTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() throws Exception {
        assertUtilityConstructor(StackTraceElementCodec.class);
    }
}
