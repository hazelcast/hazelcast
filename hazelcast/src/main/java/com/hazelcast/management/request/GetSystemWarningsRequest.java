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

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hazelcast.logging.SystemLogRecord;
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.nio.Address;
import java.util.LinkedList;
import java.util.List;

/**
 * Request for fetching system warnings.
 */
public class GetSystemWarningsRequest implements ConsoleRequest {

    public GetSystemWarningsRequest() {
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_SYSTEM_WARNINGS;
    }

    @Override
    public Object readResponse(JsonObject in) {
        List<SystemLogRecord> list = new LinkedList<SystemLogRecord>();
        String node = in.get("node").asString();
        final JsonArray logs = in.get("logs").asArray();
        for (JsonValue log : logs) {
            SystemLogRecord systemLogRecord = new SystemLogRecord();
            systemLogRecord.fromJson(log.asObject());
            systemLogRecord.setNode(node);
            list.add(systemLogRecord);
        }
        return list;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject root) {
        final JsonObject result = new JsonObject();
        List<SystemLogRecord> logBundle = mcs.getHazelcastInstance().node.getSystemLogService().getSystemWarnings();
        final Address address = mcs.getHazelcastInstance().node.getThisAddress();
        result.add("node", address.getHost() + ":" + address.getPort());
        JsonArray logs = new JsonArray();
        for (SystemLogRecord systemLogRecord : logBundle) {
            logs.add(systemLogRecord.toJson());
        }
        result.add("logs", logs);
        root.add("result", result);
    }

    @Override
    public JsonObject toJson() {
        return new JsonObject();
    }

    @Override
    public void fromJson(JsonObject json) {

    }
}
