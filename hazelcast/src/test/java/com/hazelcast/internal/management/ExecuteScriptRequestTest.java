package com.hazelcast.internal.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.request.ExecuteScriptRequest;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ExecuteScriptRequestTest extends HazelcastTestSupport {

    private Cluster cluster;
    private ManagementCenterService managementCenterService;
    private Map<String, Object> bindings = new HashMap<String, Object>();

    /**
     * Zulu 6 and 7 doesn't have Rhino script engine, so this test should be excluded.
     * see http://zulu.org/forum/thread/nullpointerexception-on-loading-the-javascript-engine-in-zulu-7-u76/
     */
    @Rule
    public ZuluExcludeRule zuluExcludeRule = new ZuluExcludeRule();

    @Before
    public void setUp() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances();

        cluster = instances[0].getCluster();
        managementCenterService = getNode(instances[0]).getManagementCenterService();

        bindings.put("key", "value");
    }

    @Test
    public void testExecuteScriptRequest() throws Exception {
        ExecuteScriptRequest request = new ExecuteScriptRequest("print('test');", "JavaScript", false, bindings);

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        String response = (String) request.readResponse(result);
        assertEquals("", response.trim());
    }

    @Test
    public void testExecuteScriptRequest_whenTargetAllMembers() throws Exception {
        ExecuteScriptRequest request = new ExecuteScriptRequest("print('test');", "JavaScript", true, null);

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        String response = (String) request.readResponse(result);
        assertEquals(format("error%nerror%n"), response);
    }

    @Test
    public void testExecuteScriptRequest_whenTargetAllMembers_withTarget() throws Exception {
        Address address = cluster.getLocalMember().getAddress();
        Set<String> targets = Collections.singleton(address.getHost() + ":" + address.getPort());
        ExecuteScriptRequest request = new ExecuteScriptRequest("print('test');", "JavaScript", targets, bindings);

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        String response = (String) request.readResponse(result);
        assertEquals("error", response.trim());
    }

    @Test
    public void testExecuteScriptRequest_withIllegalScriptEngine() throws Exception {
        ExecuteScriptRequest request = new ExecuteScriptRequest("script", "engine", true, bindings);

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        String response = (String) request.readResponse(result);
        assertTrue(response.contains("IllegalArgumentException"));
    }

    @Test
    public void testExecuteScriptRequest_withScriptException() throws Exception {
        ExecuteScriptRequest request = new ExecuteScriptRequest("print(;", "JavaScript", true, bindings);

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        String response = (String) request.readResponse(result);
        assertNotNull(response);
    }
}
