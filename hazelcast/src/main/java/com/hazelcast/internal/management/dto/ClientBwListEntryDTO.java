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

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.json.internal.JsonSerializable;

import static com.hazelcast.internal.util.JsonUtil.getString;

/**
 * A serializable DTO for client B/W list filtering configuration entries.
 */
public final class ClientBwListEntryDTO implements JsonSerializable {

    public Type type;
    public String value;

    public ClientBwListEntryDTO() {
    }

    public ClientBwListEntryDTO(Type type, String value) {
        this.type = type;
        this.value = value;
    }

    public Type getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("type", type.toString());
        root.add("value", value);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        String typeStr = getString(json, "type");
        type = Type.valueOf(typeStr);
        value = getString(json, "value");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClientBwListEntryDTO that = (ClientBwListEntryDTO) o;

        if (type != that.type) {
            return false;
        }
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    public enum Type {

        IP_ADDRESS(0), INSTANCE_NAME(1), LABEL(2);

        private final int id;

        Type(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public static Type getById(final int id) {
            for (Type type : values()) {
                if (type.id == id) {
                    return type;
                }
            }
            return null;
        }

    }

}
