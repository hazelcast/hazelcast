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
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.management.operation.ThreadDumpOperation;

public class ThreadDumpRequest implements ConsoleRequest {

    private boolean dumpDeadlocks;

    public ThreadDumpRequest() {
    }

    public ThreadDumpRequest(boolean dumpDeadlocks) {
        this.dumpDeadlocks = dumpDeadlocks;
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_GET_THREAD_DUMP;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject root) {
        final JsonObject result = new JsonObject();
        String threadDump = (String) mcs.callOnThis(new ThreadDumpOperation(dumpDeadlocks));
        if (threadDump != null) {
            result.add("hasDump", true);
            result.add("dump", threadDump);
        } else {
            result.add("hasDump", false);
        }
        root.add("result", result);

    }

    @Override
    public String readResponse(JsonObject in) {
        final boolean hasDump = in.get("hasDump").asBoolean();
        if (hasDump) {
            return in.get("dump").asString();
        }
        return null;
    }

    @Override
    public JsonValue toJson() {
        final JsonObject root = new JsonObject();
        root.add("dumpDeadlocks", dumpDeadlocks);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        dumpDeadlocks = json.get("dumpDeadlocks").asBoolean();
    }
}
