package com.hazelcast.internal.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.operation.ChangeWanStateOperation;
import com.hazelcast.internal.management.request.ChangeWanStateRequest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ChangeWanStateRequestTest extends HazelcastTestSupport {

    private ManagementCenterService managementCenterService;

    @Before
    public void setUp() {
        HazelcastInstance hz = createHazelcastInstance();
        Node node = getNode(hz);
        managementCenterService = node.getManagementCenterService();
    }

    @Test
    public void testOperationDefaultConstructor() {
        ChangeWanStateOperation operation = new ChangeWanStateOperation();
        assertNotNull(operation);
    }

    @Test
    public void testResumingWanState() throws Exception {
        ChangeWanStateRequest changeWanStateRequest = new ChangeWanStateRequest("schema", "publisher", true);
        JsonObject jsonObject = new JsonObject();
        changeWanStateRequest.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        assertNotEquals(ChangeWanStateRequest.SUCCESS, changeWanStateRequest.readResponse(result));
    }

    @Test
    public void testPausingWanState() throws Exception {
        ChangeWanStateRequest changeWanStateRequest = new ChangeWanStateRequest("schema", "publisher", false);
        JsonObject jsonObject = new JsonObject();
        changeWanStateRequest.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        assertNotEquals(ChangeWanStateRequest.SUCCESS, changeWanStateRequest.readResponse(result));
    }

    @Test
    public void testSerialization() throws IllegalAccessException {
        ChangeWanStateRequest changeWanStateRequest1 = new ChangeWanStateRequest("schema", "publisher", false);
        JsonObject jsonObject = changeWanStateRequest1.toJson();

        ChangeWanStateRequest changeWanStateRequest2 = new ChangeWanStateRequest();
        changeWanStateRequest2.fromJson(jsonObject);

        assertEquals(changeWanStateRequest1.getPublisherName(), changeWanStateRequest2.getPublisherName());
        assertEquals(changeWanStateRequest1.getSchemeName(), changeWanStateRequest2.getSchemeName());
        assertEquals(changeWanStateRequest1.isStart(), changeWanStateRequest2.isStart());
    }
}
