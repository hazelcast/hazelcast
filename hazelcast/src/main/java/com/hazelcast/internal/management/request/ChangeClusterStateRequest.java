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

package com.hazelcast.internal.management.request;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.logging.ILogger;

import static com.hazelcast.internal.util.JsonUtil.getString;

/**
 * Request coming from Management Center for changing the {@link ClusterState}
 */
public class ChangeClusterStateRequest implements AsyncConsoleRequest {

    private static final String FAILURE = "FAILURE: ";

    private String state;

    public ChangeClusterStateRequest() {
    }

    public ChangeClusterStateRequest(String state) {
        this.state = state;
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_CHANGE_CLUSTER_STATE;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject out) throws Exception {
        String resultString = "SUCCESS";
        try {
            Cluster cluster = mcs.getHazelcastInstance().getCluster();
            cluster.changeClusterState(getClusterState(state));
        } catch (Exception e) {
            ILogger logger = mcs.getHazelcastInstance().node.nodeEngine.getLogger(getClass());
            logger.warning("Cluster state can not be changed: ", e);
            resultString = FAILURE + e.getMessage();
        }
        JsonObject result = new JsonObject().add("result", resultString);
        out.add("result", result);
    }

    private static ClusterState getClusterState(String state) {
        return ClusterState.valueOf(state);
    }

    @Override
    public void fromJson(JsonObject json) {
        state = getString(json, "state");
    }
}
