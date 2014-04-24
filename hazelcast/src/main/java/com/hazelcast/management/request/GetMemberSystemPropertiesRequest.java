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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class GetMemberSystemPropertiesRequest implements ConsoleRequest {

    public GetMemberSystemPropertiesRequest() {
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_MEMBER_SYSTEM_PROPERTIES;
    }

    @Override
    public Object readResponse(JsonObject in) {
        Map<String, String> properties = new LinkedHashMap<String, String>();
        final Iterator<JsonObject.Member> iterator = in.iterator();
        while (iterator.hasNext()) {
            final JsonObject.Member property = iterator.next();
            properties.put(property.getName(), property.getValue().asString());
        }
        return properties;
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
    public JsonValue toJson() {
        return new JsonObject();
    }

    @Override
    public void fromJson(JsonObject json) {

    }
}
