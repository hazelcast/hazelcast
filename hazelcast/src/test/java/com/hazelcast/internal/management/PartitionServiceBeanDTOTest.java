package com.hazelcast.internal.management;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.dto.PartitionServiceBeanDTO;
import com.hazelcast.monitor.impl.MemberStateImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PartitionServiceBeanDTOTest extends HazelcastTestSupport {

    /**
     * https://github.com/hazelcast/hazelcast/issues/8463
     */
    @Test
    public void testJMXStatsWithPublicAddressHostName() {
        Config config = new Config();
        config.getNetworkConfig().setPublicAddress("hazelcast.org");
        HazelcastInstance instance = createHazelcastInstance(config);
        warmUpPartitions(instance);
        MemberStateImpl memberState = new MemberStateImpl();
        TimedMemberStateFactoryHelper.registerJMXBeans(getNode(instance).hazelcastInstance, memberState);
        PartitionServiceBeanDTO partitionServiceDTO = memberState.getMXBeans().getPartitionServiceBean();
        assertEquals(partitionServiceDTO.getPartitionCount(), partitionServiceDTO.getActivePartitionCount());
    }
}
