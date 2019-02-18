/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Client;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.management.JsonSerializable;

import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.util.JsonUtil.getArray;
import static com.hazelcast.util.JsonUtil.getString;

/**
 * A Serializable DTO for {@link com.hazelcast.client.impl.ClientEndpoint}.
 */
public class ClientEndPointDTO implements JsonSerializable {

    public String uuid;
    public String address;
    public String clientType;
    public String name;
    public Set<String> labels;

    public ClientEndPointDTO() {
    }

    public ClientEndPointDTO(Client client) {
        this.uuid = client.getUuid();
        this.address = client.getSocketAddress().getHostName() + ":" + client.getSocketAddress().getPort();
        this.clientType = client.getClientType().toString();
        this.name = client.getName();
        this.labels = client.getLabels();
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = Json.object();
        root.add("uuid", uuid);
        root.add("address", address);
        root.add("clientType", clientType);
        root.add("name", name);
        JsonArray labelsObject = Json.array();
        for (String label : labels) {
            labelsObject.add(label);
        }
        root.add("labels", labelsObject);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        uuid = getString(json, "uuid");
        address = getString(json, "address");
        clientType = getString(json, "clientType");
        name = getString(json, "name");
        JsonArray labelsArray = getArray(json, "labels");
        labels = new HashSet<String>();
        for (JsonValue labelValue : labelsArray) {
            labels.add(labelValue.asString());
        }
    }
}
