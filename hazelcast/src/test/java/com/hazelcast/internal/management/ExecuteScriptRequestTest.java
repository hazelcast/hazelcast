/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

import static com.hazelcast.util.JsonUtil.getString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
        String response = getString(result, "scriptResult");
        assertEquals("", response.trim());
    }

    @Test
    public void testExecuteScriptRequest_whenTargetAllMembers() throws Exception {
        ExecuteScriptRequest request = new ExecuteScriptRequest("print('test');", "JavaScript", true, null);

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        String response = getString(result, "scriptResult");
        assertEquals("error\nerror\n", response);
    }

    @Test
    public void testExecuteScriptRequest_whenTargetAllMembers_withTarget() throws Exception {
        Address address = cluster.getLocalMember().getAddress();
        Set<String> targets = Collections.singleton(address.getHost() + ":" + address.getPort());
        ExecuteScriptRequest request = new ExecuteScriptRequest("print('test');", "JavaScript", targets, bindings);

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        String response = getString(result, "scriptResult");
        assertEquals("error", response.trim());
    }

    @Test
    public void testExecuteScriptRequest_withIllegalScriptEngine() throws Exception {
        ExecuteScriptRequest request = new ExecuteScriptRequest("script", "engine", true, bindings);

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        String response = getString(result, "scriptResult");
        assertContains(response, "IllegalArgumentException");
    }

    @Test
    public void testExecuteScriptRequest_withScriptException() throws Exception {
        ExecuteScriptRequest request = new ExecuteScriptRequest("print(;", "JavaScript", true, bindings);

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        String response = getString(result, "scriptResult");
        assertNotNull(response);
    }
}
