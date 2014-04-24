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
import com.hazelcast.core.EntryView;
import com.hazelcast.core.IMap;
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class GetMapEntryRequest implements ConsoleRequest {

    private String mapName;
    private String type;
    private String key;

    public GetMapEntryRequest() {
    }

    public GetMapEntryRequest(String type, String mapName, String key) {
        this.type = type;
        this.mapName = mapName;
        this.key = key;
    }
    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_MAP_ENTRY;
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
    public void writeResponse(ManagementCenterService mcs, JsonObject root) throws Exception {
        IMap map = mcs.getHazelcastInstance().getMap(mapName);
        JsonObject result = new JsonObject();
        EntryView entry = null;
        if (type.equals("string")) {
            entry = map.getEntryView(key);
        } else if (type.equals("long")) {
            entry = map.getEntryView(Long.valueOf(key));
        } else if (type.equals("integer")) {
            entry = map.getEntryView(Integer.valueOf(key));
        }
        if (entry != null) {
            Object value = entry.getValue();
            result.add("browse_value", value != null ? value.toString() : "null");
            result.add("browse_class", value != null ? value.getClass().getName() : "null");
            result.add("memory_cost", Long.toString(entry.getCost()));
            result.add("date_creation_time", Long.toString(entry.getCreationTime()));
            result.add("date_expiration_time", Long.toString(entry.getExpirationTime()));
            result.add("browse_hits", Long.toString(entry.getHits()));
            result.add("date_access_time", Long.toString(entry.getLastAccessTime()));
            result.add("date_update_time", Long.toString(entry.getLastUpdateTime()));
            result.add("browse_version", Long.toString(entry.getVersion()));
        }
        root.add("result", result);
    }

    @Override
    public JsonValue toJson() {
        JsonObject root = new JsonObject();
        root.add("mapName", mapName);
        root.add("type", type);
        root.add("key", key);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        mapName = json.get("mapName").asString();
        type = json.get("type").asString();
        key = json.get("key").asString();
    }
}
