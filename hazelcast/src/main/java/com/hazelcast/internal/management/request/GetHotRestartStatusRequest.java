/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.hotrestart.InternalHotRestartService;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;

import java.io.IOException;

/**
 * Request to gather and present Hot Restart status on Management Center
 */
public class GetHotRestartStatusRequest implements ConsoleRequest {

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_GET_HOTRESTART_STATUS;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject out) throws Exception {
        final Node node = mcs.getHazelcastInstance().node;
        final InternalHotRestartService hotRestartService = node.getNodeExtension().getInternalHotRestartService();
        out.add("result", hotRestartService.getCurrentClusterHotRestartStatus().toJson());
    }

    @Override
    public Object readResponse(JsonObject in) throws IOException {
        JsonObject result = (JsonObject) in.get("result");
        ClusterHotRestartStatusDTO hotRestartStatus = new ClusterHotRestartStatusDTO();
        hotRestartStatus.fromJson(result);
        return hotRestartStatus;
    }

    @Override
    public JsonObject toJson() {
        return new JsonObject();
    }

    @Override
    public void fromJson(JsonObject json) {
    }
}
