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

import com.hazelcast.config.WanPublisherState;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.operation.ChangeWanStateOperation;
import com.hazelcast.internal.json.JsonObject;

import static com.hazelcast.internal.management.ManagementCenterService.resolveFuture;
import static com.hazelcast.util.JsonUtil.getString;

/**
 * Request coming from Management Center for {@link ChangeWanStateOperation}
 */
public class ChangeWanStateRequest implements ConsoleRequest {

    /**
     * Result message when {@link ChangeWanStateOperation} is invoked successfully
     */
    public static final String SUCCESS = "success";

    private String schemeName;
    private String publisherName;
    private WanPublisherState state;

    public ChangeWanStateRequest() {
    }

    public ChangeWanStateRequest(String schemeName, String publisherName, WanPublisherState state) {
        this.schemeName = schemeName;
        this.publisherName = publisherName;
        this.state = state;
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_WAN_PUBLISHER;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject out) {
        Object operationResult = resolveFuture(
                mcs.callOnThis(new ChangeWanStateOperation(schemeName, publisherName, state)));
        JsonObject result = new JsonObject();
        if (operationResult == null) {
            result.add("result", SUCCESS);
        } else {
            result.add("result", operationResult.toString());
        }
        out.add("result", result);
    }

    @Override
    public void fromJson(JsonObject json) {
        schemeName = getString(json, "schemeName");
        publisherName = getString(json, "publisherName");
        state = WanPublisherState.valueOf(getString(json, "state"));
    }
}
