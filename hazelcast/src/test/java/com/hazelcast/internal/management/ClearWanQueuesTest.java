package com.hazelcast.internal.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.operation.ChangeWanStateOperation;
import com.hazelcast.internal.management.operation.ClearWanQueuesOperation;
import com.hazelcast.internal.management.request.ChangeWanStateRequest;
import com.hazelcast.internal.management.request.ClearWanQueuesRequest;
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
public class ClearWanQueuesTest extends HazelcastTestSupport {

    private ManagementCenterService managementCenterService;

    @Before
    public void setUp() {
        HazelcastInstance hz = createHazelcastInstance();
        Node node = getNode(hz);
        managementCenterService = node.getManagementCenterService();
    }

    @Test
    public void testOperationDefaultConstructor() {
        ClearWanQueuesOperation operation = new ClearWanQueuesOperation();
        assertNotNull(operation);
    }

    @Test
    public void testResumingWanState() throws Exception {
        ClearWanQueuesRequest clearWanQueuesRequest = new ClearWanQueuesRequest("schema", "publisher");
        JsonObject jsonObject = new JsonObject();
        clearWanQueuesRequest.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        assertNotEquals(ChangeWanStateRequest.SUCCESS, clearWanQueuesRequest.readResponse(result));
    }

    @Test
    public void testSerialization() throws IllegalAccessException {
        ClearWanQueuesRequest clearWanQueuesRequest1 = new ClearWanQueuesRequest("schema", "publisher");
        JsonObject jsonObject = clearWanQueuesRequest1.toJson();

        ClearWanQueuesRequest clearWanQueuesRequest2 = new ClearWanQueuesRequest();
        clearWanQueuesRequest2.fromJson(jsonObject);

        assertEquals(clearWanQueuesRequest1.getPublisherName(), clearWanQueuesRequest2.getPublisherName());
        assertEquals(clearWanQueuesRequest1.getSchemeName(), clearWanQueuesRequest2.getSchemeName());
    }
}
