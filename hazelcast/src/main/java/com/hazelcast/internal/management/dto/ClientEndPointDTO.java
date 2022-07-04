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

import com.hazelcast.client.Client;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.json.internal.JsonSerializable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.internal.util.JsonUtil.getArray;
import static com.hazelcast.internal.util.JsonUtil.getString;

/**
 * A Serializable DTO for {@link com.hazelcast.client.impl.ClientEndpoint}.
 */
public class ClientEndPointDTO implements JsonSerializable {

    public UUID uuid;

    /**
     * Client's socket address as "hostname:port"
     */
    public String address;
    public String clientType;
    public String name;
    public Set<String> labels;

    /**
     * Client's IP address (without port) or {@code null} if client's address is unresolved
     */
    public String ipAddress;

    /**
     * Client's FQDN if we're able to resolve it, host name if not. {@code null} if client's address is unresolved
     */
    public String canonicalHostName;

    public ClientEndPointDTO() {
    }

    public ClientEndPointDTO(Client client) {
        this.uuid = client.getUuid();
        this.clientType = client.getClientType();
        this.name = client.getName();
        this.labels = client.getLabels();

        InetSocketAddress socketAddress = client.getSocketAddress();
        this.address = socketAddress.getHostName() + ":" + socketAddress.getPort();

        InetAddress address = socketAddress.getAddress();
        this.ipAddress = address != null ? address.getHostAddress() : null;
        this.canonicalHostName = address != null ? address.getCanonicalHostName() : null;
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = Json.object();
        root.add("uuid", uuid.toString());
        root.add("address", address);
        root.add("clientType", clientType);
        root.add("name", name);
        JsonArray labelsObject = Json.array();
        for (String label : labels) {
            labelsObject.add(label);
        }
        root.add("labels", labelsObject);
        root.add("ipAddress", ipAddress);
        root.add("canonicalHostName", canonicalHostName);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        uuid = UUID.fromString(getString(json, "uuid"));
        address = getString(json, "address");
        clientType = getString(json, "clientType");
        name = getString(json, "name");
        JsonArray labelsArray = getArray(json, "labels");
        labels = new HashSet<>();
        for (JsonValue labelValue : labelsArray) {
            labels.add(labelValue.asString());
        }
        ipAddress = getString(json, "ipAddress", null);
        canonicalHostName = getString(json, "canonicalHostName", null);
    }
}
