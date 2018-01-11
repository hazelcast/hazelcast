/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.request.GetClusterStateRequest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.JsonUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class GetClusterStateRequestTest extends HazelcastTestSupport {

    private Cluster cluster;
    private ManagementCenterService managementCenterService;

    @Before
    public void setUp() {
        HazelcastInstance hz = createHazelcastInstance();
        cluster = hz.getCluster();
        managementCenterService = getNode(hz).getManagementCenterService();
    }

    @Test
    public void testGetClusterState() throws Exception {
        GetClusterStateRequest request = new GetClusterStateRequest();

        ClusterState clusterState = cluster.getClusterState();
        JsonObject jsonObject = new JsonObject();
        request.writeResponse(managementCenterService, jsonObject);

        JsonObject result = (JsonObject) jsonObject.get("result");
        assertEquals(clusterState.name(), JsonUtil.getString(result, "result"));
    }
}
