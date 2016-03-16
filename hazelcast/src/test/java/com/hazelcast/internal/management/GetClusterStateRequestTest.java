package com.hazelcast.internal.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.request.GetClusterStateRequest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class GetClusterStateRequestTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private Cluster cluster;
    private GetClusterStateRequest getClusterStateRequest;
    private ManagementCenterService managementCenterService;

    @Before
    public void setUp() {
        hz = createHazelcastInstance();
        Node node = getNode(hz);
        managementCenterService = node.getManagementCenterService();
        cluster = hz.getCluster();
        getClusterStateRequest = new GetClusterStateRequest();
    }

    @Test
    public void testGetClusterState() throws Exception {
        ClusterState clusterState = cluster.getClusterState();
        JsonObject jsonObject = new JsonObject();
        getClusterStateRequest.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        assertEquals(clusterState.name(), getClusterStateRequest.readResponse(result));
    }
}
