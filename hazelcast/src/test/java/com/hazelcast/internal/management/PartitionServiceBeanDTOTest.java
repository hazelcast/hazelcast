package com.hazelcast.internal.management;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.dto.PartitionServiceBeanDTO;
import com.hazelcast.monitor.impl.MemberStateImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class PartitionServiceBeanDTOTest extends HazelcastTestSupport {

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test //https://github.com/hazelcast/hazelcast/issues/8463
    public void testJMXStatsWithPublicAdressHostName() {
        Config config = new Config();
        config.getNetworkConfig().setPublicAddress("hazelcast.org");
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        MemberStateImpl memberState = new MemberStateImpl();
        TimedMemberStateFactoryHelper.registerJMXBeans(getNode(instance).hazelcastInstance, memberState);
        PartitionServiceBeanDTO partitionServiceDTO = memberState.getMXBeans().getPartitionServiceBean();
        assertEquals(partitionServiceDTO.getPartitionCount(), partitionServiceDTO.getActivePartitionCount());
    }

}
