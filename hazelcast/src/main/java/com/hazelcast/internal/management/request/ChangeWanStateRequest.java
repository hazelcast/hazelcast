/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.operation.ChangeWanStateOperation;

import java.io.IOException;

import static com.hazelcast.util.JsonUtil.getBoolean;
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
    private boolean start;

    public ChangeWanStateRequest() {
    }

    public ChangeWanStateRequest(String schemeName, String publisherName, boolean start) {
        this.schemeName = schemeName;
        this.publisherName = publisherName;
        this.start = start;
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_WAN_PUBLISHER;
    }

    @Override
    public Object readResponse(JsonObject in) throws IOException {
        return getString(in, "result", "FAILURE");
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject out) throws Exception {
        ChangeWanStateOperation changeWanStateOperation =
                new ChangeWanStateOperation(schemeName, publisherName, start);
        Object operationResult = mcs.callOnThis(changeWanStateOperation);
        JsonObject result = new JsonObject();
        if (operationResult == null) {
            result.add("result", SUCCESS);
        } else {
            result.add("result", operationResult.toString());
        }
        out.add("result", result);
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("schemeName", schemeName);
        root.add("publisherName", publisherName);
        root.add("start", start);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        schemeName = getString(json, "schemeName");
        publisherName = getString(json, "publisherName");
        start = getBoolean(json, "start");
    }

    public String getSchemeName() {
        return schemeName;
    }

    public String getPublisherName() {
        return publisherName;
    }

    public boolean isStart() {
        return start;
    }
}
