package com.hazelcast.aws.utility;

import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

public class AwsURLEncoderTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(AwsURLEncoder.class);
    }
}
