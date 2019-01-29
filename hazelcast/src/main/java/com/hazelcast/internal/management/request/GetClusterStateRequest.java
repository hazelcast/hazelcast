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

package com.hazelcast.internal.management.request;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.internal.management.ManagementCenterService;

/**
 * Request coming from Management Center for getting the {@link ClusterState}
 */
public class GetClusterStateRequest implements ConsoleRequest {

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_GET_CLUSTER_STATE;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject out) throws Exception {
        ClusterState clusterState = mcs.getHazelcastInstance().getCluster().getClusterState();
        JsonObject result = new JsonObject();
        result.add("result", clusterState.toString());
        out.add("result", result);
    }

    @Override
    public void fromJson(JsonObject json) {
    }
}
