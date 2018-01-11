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
import com.hazelcast.core.EntryView;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.management.ManagementCenterService;

import static com.hazelcast.util.JsonUtil.getString;

/**
 * Request for fetching map entries.
 */
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
    public void fromJson(JsonObject json) {
        mapName = getString(json, "mapName");
        type = getString(json, "type");
        key = getString(json, "key");
    }
}
