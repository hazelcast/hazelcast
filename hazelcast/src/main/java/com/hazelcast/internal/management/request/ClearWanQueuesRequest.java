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

import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.operation.ClearWanQueuesOperation;
import com.hazelcast.internal.json.JsonObject;

import static com.hazelcast.internal.management.ManagementCenterService.resolveFuture;
import static com.hazelcast.util.JsonUtil.getString;

/**
 * Request coming from Management Center for {@link ClearWanQueuesRequest}
 */
public class ClearWanQueuesRequest implements ConsoleRequest {

    /**
     * Result message when {@link ClearWanQueuesRequest} is invoked successfully
     */
    public static final String SUCCESS = "success";

    private String schemeName;
    private String publisherName;

    public ClearWanQueuesRequest() {
    }

    public ClearWanQueuesRequest(String schemeName, String publisherName) {
        this.schemeName = schemeName;
        this.publisherName = publisherName;
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_CLEAR_WAN_QUEUES;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject out) {
        ClearWanQueuesOperation operation = new ClearWanQueuesOperation(schemeName, publisherName);
        Object operationResult = resolveFuture(mcs.callOnThis(operation));
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
    }
}
