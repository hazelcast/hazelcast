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

import com.hazelcast.core.EntryView;
import com.hazelcast.core.IMap;
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

// author: sancar - 12.12.2012
public class GetMapEntryRequest implements ConsoleRequest {

    private String mapName;
    private String type;
    private String key;

    public GetMapEntryRequest() {
        super();
    }

    public GetMapEntryRequest(String type, String mapName, String key) {
        this.type = type;
        this.mapName = mapName;
        this.key = key;
    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_MAP_ENTRY;
    }

    public Object readResponse(ObjectDataInput in) throws IOException {
        Map<String, String> properties = new LinkedHashMap<String, String>();
        int size = in.readInt();
        String[] temp;
        for (int i = 0; i < size; i++) {
            temp = in.readUTF().split(":#");
            properties.put(temp[0], temp.length == 1 ? "" : temp[1]);
        }
        return properties;
    }

    public void writeResponse(ManagementCenterService mcs, ObjectDataOutput dos) throws Exception {
        IMap map = mcs.getHazelcastInstance().getMap(mapName);

        EntryView entry = null;
        if (type.equals("string")) {
            entry = map.getEntryView(key);
        } else if (type.equals("long")) {
            entry = map.getEntryView(Long.valueOf(key));
        } else if (type.equals("integer")) {
            entry = map.getEntryView(Integer.valueOf(key));
        }

        TreeMap result = new TreeMap();

        if (entry == null) {
            result.put("No Value Found!", " ");
        } else {
            Object value = entry.getValue();
            result.put("browse_value", value != null ? value.toString() : "null");
            result.put("browse_class", value != null ? value.getClass().getName() : "null");
            result.put("memory_cost", Long.toString(entry.getCost()));
            result.put("date_creation_time", Long.toString(entry.getCreationTime()));
            result.put("date_expiration_time", Long.toString(entry.getExpirationTime()));
            result.put("browse_hits", Long.toString(entry.getHits()));
            result.put("date_access_time", Long.toString(entry.getLastAccessTime()));
            result.put("date_update_time", Long.toString(entry.getLastUpdateTime()));
            result.put("browse_version", Long.toString(entry.getVersion()));
        }

        dos.writeInt(result.size());

        for (Object property : result.keySet()) {
            dos.writeUTF(property + ":#" + result.get(property));
        }

    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(type);
        out.writeUTF(mapName);
        out.writeUTF(key);
    }

    public void readData(ObjectDataInput in) throws IOException {
        type = in.readUTF();
        mapName = in.readUTF();
        key = in.readUTF();
    }
}
