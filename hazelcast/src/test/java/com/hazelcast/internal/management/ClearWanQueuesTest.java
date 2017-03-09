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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
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
    public void testClearWanQueue() throws Exception {
        ClearWanQueuesRequest clearWanQueuesRequest = new ClearWanQueuesRequest("schema", "publisher");
        JsonObject jsonObject = new JsonObject();
        clearWanQueuesRequest.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        assertNotEquals(ChangeWanStateRequest.SUCCESS, clearWanQueuesRequest.readResponse(result));
    }

    @Test
    public void testSerialization() {
        ClearWanQueuesRequest clearWanQueuesRequest1 = new ClearWanQueuesRequest("schema", "publisher");
        JsonObject jsonObject = clearWanQueuesRequest1.toJson();

        ClearWanQueuesRequest clearWanQueuesRequest2 = new ClearWanQueuesRequest();
        clearWanQueuesRequest2.fromJson(jsonObject);

        assertEquals(clearWanQueuesRequest1.getPublisherName(), clearWanQueuesRequest2.getPublisherName());
        assertEquals(clearWanQueuesRequest1.getSchemeName(), clearWanQueuesRequest2.getSchemeName());
    }
}
