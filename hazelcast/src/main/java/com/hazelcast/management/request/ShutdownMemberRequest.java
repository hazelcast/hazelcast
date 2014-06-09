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
import com.hazelcast.management.ManagementCenterService;

import static com.hazelcast.util.JsonUtil.getString;

public class ShutdownMemberRequest implements ConsoleRequest {

    public ShutdownMemberRequest() {
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_MEMBER_SHUTDOWN;
    }

    @Override
    public Object readResponse(JsonObject json) {
        return getString(json, "result", "successful");
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject root) {
        mcs.getHazelcastInstance().getLifecycleService().shutdown();
        JsonObject result = new JsonObject();
        result.add("result", "successful");
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
