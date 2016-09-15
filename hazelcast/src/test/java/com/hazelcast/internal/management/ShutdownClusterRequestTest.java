package com.hazelcast.internal.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.internal.management.request.ShutdownClusterRequest;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ShutdownClusterRequestTest extends HazelcastTestSupport {

    private LifecycleService lifecycleService;
    private Cluster cluster;
    private ManagementCenterService managementCenterService;

    @Before
    public void setUp() {
        HazelcastInstance hz = createHazelcastInstance();
        lifecycleService = hz.getLifecycleService();
        cluster = hz.getCluster();
        managementCenterService = getNode(hz).getManagementCenterService();
    }

    @Test
    public void testShutdownCluster() throws Exception {
        ShutdownClusterRequest request = new ShutdownClusterRequest();

        cluster.getClusterState();
        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        assertEquals("SUCCESS", request.readResponse(result));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(lifecycleService.isRunning());
            }
        });
    }
}
