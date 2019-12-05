/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.request.ExecuteScriptRequest;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static com.hazelcast.internal.util.JsonUtil.getBoolean;
import static com.hazelcast.internal.util.JsonUtil.getObject;
import static com.hazelcast.internal.util.JsonUtil.getString;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExecuteScriptRequestTest extends HazelcastTestSupport {

    private ManagementCenterService managementCenterService;
    private String nodeAddressWithBrackets;
    private String nodeAddressWithoutBrackets;

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

        Node node = getNode(instances[0]);
        nodeAddressWithBrackets = node.getThisAddress().toString();
        nodeAddressWithoutBrackets = node.getThisAddress().getHost() + ":" + node.getThisAddress().getPort();
        managementCenterService = node.getManagementCenterService();
    }

    @Test
    public void testExecuteScriptRequest() throws Exception {
        ExecuteScriptRequest request = new ExecuteScriptRequest("print('test');", "JavaScript",
                singleton(nodeAddressWithoutBrackets));

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        JsonObject json = getObject(result, nodeAddressWithBrackets);
        assertTrue(getBoolean(json, "success"));
        assertEquals("error\n", getString(json, "result"));
    }

    @Test
    public void testExecuteScriptRequest_noTargets() throws Exception {
        ExecuteScriptRequest request = new ExecuteScriptRequest("print('test');", "JavaScript",
                Collections.<String>emptySet());

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testExecuteScriptRequest_withIllegalScriptEngine() throws Exception {
        ExecuteScriptRequest request = new ExecuteScriptRequest("script", "engine",
                singleton(nodeAddressWithoutBrackets));

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        JsonObject json = getObject(result, nodeAddressWithBrackets);
        assertFalse(getBoolean(json, "success"));
        assertNotNull(getString(json, "result"));
        assertContains(getString(json, "stackTrace"), "IllegalArgumentException");
    }

    @Test
    public void testExecuteScriptRequest_withScriptException() throws Exception {
        ExecuteScriptRequest request = new ExecuteScriptRequest("print(;", "JavaScript",
                singleton(nodeAddressWithoutBrackets));

        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        JsonObject json = getObject(result, nodeAddressWithBrackets);
        assertFalse(getBoolean(json, "success"));
        assertNotNull(getString(json, "result"));
        assertContains(getString(json, "stackTrace"), "ScriptException");
    }
}
