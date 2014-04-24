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
import com.eclipsesource.json.JsonValue;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
    public void writeResponse(ManagementCenterService managementCenterService, JsonObject dos) {
//        managementCenterService.signalVersionMismatch();
//        Node node = managementCenterService.getHazelcastInstance().node;
//        ILogger logger = node.getLogger(VersionMismatchLogRequest.class);
//        //todo: does this message make sense because to the user it just displays version information we already know.
//        //he has no clue that the management version is not matching with his own.
//        logger.severe("The version of the management center is " + manCenterVersion);
    }

    @Override
    public JsonValue toJson() {
        return null;
    }

    @Override
    public void fromJson(JsonObject json) {
    }
}
