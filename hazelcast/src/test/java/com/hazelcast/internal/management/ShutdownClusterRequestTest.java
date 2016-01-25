package com.hazelcast.internal.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.instance.Node;
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

    private final String SUCCESS = "SUCCESS";

    private HazelcastInstance hz;
    private LifecycleService lifecycleService;
    private Cluster cluster;
    private ShutdownClusterRequest shutdownClusterRequest;
    private ManagementCenterService managementCenterService;

    @Before
    public void setUp() {
        hz = createHazelcastInstance();
        lifecycleService = hz.getLifecycleService();
        Node node = getNode(hz);
        managementCenterService = node.getManagementCenterService();
        cluster = hz.getCluster();
        shutdownClusterRequest = new ShutdownClusterRequest();
    }

    @Test
    public void testShutdownCluster() throws Exception {
        ClusterState clusterState = cluster.getClusterState();
        JsonObject jsonObject = new JsonObject();
        shutdownClusterRequest.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        assertEquals(SUCCESS, shutdownClusterRequest.readResponse(result));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(lifecycleService.isRunning());
            }
        });
    }
}

