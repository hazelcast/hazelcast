package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class JoinConfigTest {

    @Test
    public void test() {
        assertNotOk(true, true, true);
        assertNotOk(true, true, false);
        assertNotOk(true, false, true);
        assertNotOk(false, true, true);

        assertOk(false, false, false);

        assertOk(true, false, false);
        assertOk(false, true, false);
        assertOk(false, false, true);
    }

    public void assertOk(boolean tcp, boolean multicast, boolean aws) {
        JoinConfig config = new JoinConfig();
        config.getMulticastConfig().setEnabled(multicast);
        config.getTcpIpConfig().setEnabled(tcp);
        config.getAwsConfig().setEnabled(aws);

        config.verify();
    }

    public void assertNotOk(boolean tcp, boolean multicast, boolean aws) {
        JoinConfig config = new JoinConfig();
        config.getMulticastConfig().setEnabled(multicast);
        config.getTcpIpConfig().setEnabled(tcp);
        config.getAwsConfig().setEnabled(aws);

        try {
            config.verify();
            fail();
        } catch (IllegalStateException e) {

        }
    }
}
