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

import com.eclipsesource.json.JsonObject;
import com.hazelcast.internal.management.ManagementCenterService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Request for invoking Garbage Collection on the node.
 */
public class RunGcRequest implements ConsoleRequest {

    public RunGcRequest() {
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_RUN_GC;
    }

    @Override
    @SuppressFBWarnings(value = "DM_GC", justification = "Explicit GC is the point of this class")
    public void writeResponse(ManagementCenterService mcs, JsonObject root) throws Exception {
        System.gc();
        root.add("result", new JsonObject());
    }

    @Override
    public void fromJson(JsonObject json) {

    }
}
