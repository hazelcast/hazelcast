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

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.request.ChangeClusterStateRequest;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.util.JsonUtil.getString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ChangeClusterStateRequestTest extends HazelcastTestSupport {

    private Cluster cluster;
    private ManagementCenterService managementCenterService;

    @Before
    public void setUp() {
        HazelcastInstance hz = createHazelcastInstance();
        cluster = hz.getCluster();
        managementCenterService = getNode(hz).getManagementCenterService();
    }

    @Test
    public void testChangeClusterState() throws Exception {
        ChangeClusterStateRequest changeClusterStateRequest = new ChangeClusterStateRequest("FROZEN");
        JsonObject jsonObject = new JsonObject();
        changeClusterStateRequest.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        assertEquals("SUCCESS", getString(result, "result"));

        assertEquals(ClusterState.valueOf("FROZEN"), cluster.getClusterState());
    }

    @Test
    public void testChangeClusterState_withInvalidState() throws Exception {
        ChangeClusterStateRequest changeClusterStateRequest = new ChangeClusterStateRequest("IN_TRANSITION");
        JsonObject jsonObject = new JsonObject();
        changeClusterStateRequest.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        String resultString = getString(result, "result");
        assertTrue(resultString.startsWith("FAILURE"));
    }

    @Test
    public void testChangeClusterState_withNonExistent() throws Exception {
        ChangeClusterStateRequest changeClusterStateRequest = new ChangeClusterStateRequest("MURAT");
        JsonObject jsonObject = new JsonObject();
        changeClusterStateRequest.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        String resultString = getString(result, "result");
        assertTrue(resultString.startsWith("FAILURE"));
    }
}
