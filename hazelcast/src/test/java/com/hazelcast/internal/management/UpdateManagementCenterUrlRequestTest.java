package com.hazelcast.internal.management;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.ascii.rest.HttpCommand;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class UpdateManagementCenterUrlRequestTest extends HazelcastTestSupport {

    private ManagementCenterService managementCenterService;

    @Before
    public void setUp() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances();

        managementCenterService = getNode(instances[0]).getManagementCenterService();
    }

    @Test
    public void testExecuteScriptRequest() throws Exception {
        byte[] result = managementCenterService.clusterWideUpdateManagementCenterUrl("dev", "dev-pass", "invalid");
        assertEquals(HttpCommand.RES_204, result);
    }
}
