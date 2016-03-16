package com.hazelcast.internal.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.request.ChangeClusterStateRequest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ChangeClusterStateRequestTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private Cluster cluster;
    private ManagementCenterService managementCenterService;

    @Before
    public void setUp() {
        hz = createHazelcastInstance();
        Node node = getNode(hz);
        managementCenterService = node.getManagementCenterService();
        cluster = hz.getCluster();
    }

    @Test
    public void testChangeClusterState() throws Exception {
        ChangeClusterStateRequest changeClusterStateRequest = new ChangeClusterStateRequest("FROZEN");
        JsonObject jsonObject = new JsonObject();
        changeClusterStateRequest.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        assertEquals("SUCCESS", changeClusterStateRequest.readResponse(result));

        assertEquals(ClusterState.valueOf("FROZEN"), cluster.getClusterState());
    }

    @Test
    public void testChangeClusterState_withInvalidState() throws Exception {
        ChangeClusterStateRequest changeClusterStateRequest = new ChangeClusterStateRequest("IN_TRANSITION");
        JsonObject jsonObject = new JsonObject();
        changeClusterStateRequest.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        String resultString = (String) changeClusterStateRequest.readResponse(result);
        assertTrue(resultString.startsWith("FAILURE"));
    }

    @Test
    public void testChangeClusterState_withNonExistent() throws Exception {
        ChangeClusterStateRequest changeClusterStateRequest = new ChangeClusterStateRequest("MURAT");
        JsonObject jsonObject = new JsonObject();
        changeClusterStateRequest.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        String resultString = (String) changeClusterStateRequest.readResponse(result);
        assertTrue(resultString.startsWith("FAILURE"));
    }
}

