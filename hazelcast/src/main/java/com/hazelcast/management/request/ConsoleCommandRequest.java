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
import com.hazelcast.management.ConsoleCommandHandler;
import com.hazelcast.management.ManagementCenterService;

import static com.hazelcast.util.JsonUtil.getString;

public class ConsoleCommandRequest implements ConsoleRequest {

    private String command;

    public ConsoleCommandRequest() {
    }

    public ConsoleCommandRequest(String command) {
        this.command = command;
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_CONSOLE_COMMAND;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject root) throws Exception {
        ConsoleCommandHandler handler = mcs.getCommandHandler();
        JsonObject result = new JsonObject();
        try {
            final String output = handler.handleCommand(command);
            result.add("output", output);
        } catch (Throwable e) {
            result.add("output", "Error: " + e.getClass().getSimpleName() + "[" + e.getMessage() + "]");
        }
        root.add("result", result);
    }

    @Override
    public Object readResponse(JsonObject json) {
        return json.get("output").asString();
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        root.add("command", command);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        command = getString(json, "command", "");
    }
}
