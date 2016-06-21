package com.hazelcast.spi.discovery.multicast;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MulticastPropertiesTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(MulticastProperties.class);
    }
}
