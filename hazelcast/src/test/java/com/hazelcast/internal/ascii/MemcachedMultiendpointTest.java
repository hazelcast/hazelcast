package com.hazelcast.internal.ascii;

import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MemcachedMultiendpointTest
        extends MemcachedTest {

    public void setup() throws Exception {
        AdvancedNetworkConfig anc = config.getAdvancedNetworkConfig();
        anc.setEnabled(true)
           .setMemcacheEndpointConfig(new ServerSocketEndpointConfig());

        // Join is disabled intentionally. will start standalone HazelcastInstances.
        JoinConfig join = anc.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);

        instance = Hazelcast.newHazelcastInstance(config);
        client = getMemcachedClient(instance);
    }

}
