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

import com.hazelcast.internal.management.ConsoleCommandHandler;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.json.JsonObject;

import static com.hazelcast.internal.util.JsonUtil.getString;

/**
 *  Request for sending console commands.
 */

public class ConsoleCommandRequest implements ConsoleRequest {

    private String command;

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
    public void fromJson(JsonObject json) {
        command = getString(json, "command", "");
    }
}
