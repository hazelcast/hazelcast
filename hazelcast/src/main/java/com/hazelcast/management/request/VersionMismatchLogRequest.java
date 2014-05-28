/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.management.request;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.management.ManagementCenterService;
import java.io.IOException;

import static com.hazelcast.util.JsonUtil.getString;

public class VersionMismatchLogRequest implements ConsoleRequest {

    private String manCenterVersion;

    public VersionMismatchLogRequest() {
    }

    public VersionMismatchLogRequest(String manCenterVersion) {
        this.manCenterVersion = manCenterVersion;
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_LOG_VERSION_MISMATCH;
    }

    @Override
    public Object readResponse(JsonObject in) throws IOException {
        return "SUCCESS";
    }

    @Override
    public void writeResponse(ManagementCenterService managementCenterService, JsonObject root) {
        managementCenterService.signalVersionMismatch();
        Node node = managementCenterService.getHazelcastInstance().node;
        ILogger logger = node.getLogger(VersionMismatchLogRequest.class);
        logger.severe("The version of the management center is " + manCenterVersion);
        root.add("result", new JsonObject());
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        root.add("manCenterVersion", manCenterVersion);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        manCenterVersion = getString(json, "manCenterVersion");
    }
}
