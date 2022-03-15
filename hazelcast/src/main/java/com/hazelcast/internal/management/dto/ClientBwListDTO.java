/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management.dto;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.json.internal.JsonSerializable;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.util.JsonUtil.getArray;
import static com.hazelcast.internal.util.JsonUtil.getString;

/**
 * A serializable DTO that describes client B/W list filtering configuration received from Management Center.
 */
public class ClientBwListDTO implements JsonSerializable {

    public Mode mode;
    public List<ClientBwListEntryDTO> entries;

    public ClientBwListDTO() {
    }

    public ClientBwListDTO(Mode mode, List<ClientBwListEntryDTO> entries) {
        this.mode = mode;
        this.entries = entries;
    }

    @Override
    public JsonObject toJson() {
        JsonObject object = new JsonObject();
        object.add("mode", mode.toString());

        if (entries != null) {
            JsonArray entriesArray = new JsonArray();
            for (ClientBwListEntryDTO entry : entries) {
                JsonObject json = entry.toJson();
                if (json != null) {
                    entriesArray.add(json);
                }
            }
            object.add("entries", entriesArray);
        }

        return object;
    }

    @Override
    public void fromJson(JsonObject json) {
        String modeStr = getString(json, "mode");
        mode = Mode.valueOf(modeStr);

        entries = new ArrayList<>();
        JsonArray entriesArray = getArray(json, "entries");
        for (JsonValue jsonValue : entriesArray) {
            ClientBwListEntryDTO entryDTO = new ClientBwListEntryDTO();
            entryDTO.fromJson(jsonValue.asObject());
            entries.add(entryDTO);
        }
    }

    public enum Mode {

        DISABLED(0), WHITELIST(1), BLACKLIST(2);

        private final int id;

        Mode(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public static Mode getById(final int id) {
            for (Mode mode : values()) {
                if (mode.id == id) {
                    return mode;
                }
            }
            return null;
        }

    }

}
