package com.hazelcast.aws.impl;

import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

public class ConstantsTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() throws Exception {
        assertUtilityConstructor(Constants.class);
    }
}
