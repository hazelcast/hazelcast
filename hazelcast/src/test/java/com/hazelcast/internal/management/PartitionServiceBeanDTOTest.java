package com.hazelcast.internal.management;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.dto.PartitionServiceBeanDTO;
import com.hazelcast.monitor.impl.MemberStateImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PartitionServiceBeanDTOTest extends HazelcastTestSupport {

    private HazelcastInstance instance;

    @After
    public void shutdown() {
        if (instance != null) {
            instance.shutdown();
        }
    }

    /**
     * https://github.com/hazelcast/hazelcast/issues/8463
     */
    @Test
    public void testJMXStatsWithPublicAddressHostName() {
        String publicAddress = "hazelcast.org";
        instance = createInstanceWithPublicAddress(publicAddress);
        assumeNotNull(instance, "Internet access and/or DNS resolver are not available, "
                + publicAddress + " cannot be resolved!");

        warmUpPartitions(instance);

        MemberStateImpl memberState = new MemberStateImpl();
        TimedMemberStateFactoryHelper.registerJMXBeans(getNode(instance).hazelcastInstance, memberState);
        PartitionServiceBeanDTO partitionServiceDTO = memberState.getMXBeans().getPartitionServiceBean();
        assertEquals(partitionServiceDTO.getPartitionCount(), partitionServiceDTO.getActivePartitionCount());
    }

    private HazelcastInstance createInstanceWithPublicAddress(String publicAddress) {
        try {
            Config config = getConfig();
            NetworkConfig networkConfig = config.getNetworkConfig();
            networkConfig.setPublicAddress(publicAddress);
            return Hazelcast.newHazelcastInstance(config);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof UnknownHostException) {
                // internet access and/or DNS resolver are not available
                return null;
            }
            throw e;
        }
    }

    @Override
    protected Config getConfig() {
        Config config = new Config();

        // Join is disabled intentionally. will start standalone HazelcastInstance.
        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);

        return config;
    }
}
