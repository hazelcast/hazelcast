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

import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.json.JsonObject;

import java.util.Map;
import java.util.Properties;

/**
 * Request for fetching member system properties.
 */
public class GetMemberSystemPropertiesRequest implements ConsoleRequest {

    public GetMemberSystemPropertiesRequest() {
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_MEMBER_SYSTEM_PROPERTIES;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject root) {
        Properties properties = System.getProperties();
        JsonObject result = new JsonObject();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            result.add(entry.getKey().toString(), entry.getValue().toString());
        }
        root.add("result", result);
    }

    @Override
    public void fromJson(JsonObject json) {

    }
}
