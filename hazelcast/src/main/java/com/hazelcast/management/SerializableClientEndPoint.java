/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.core.Client;

import static com.hazelcast.util.JsonUtil.getString;

/**
 * A Serializable DTO for {@link com.hazelcast.client.ClientEndpoint}.
 */
public class SerializableClientEndPoint implements JsonSerializable {

    String uuid;
    String address;
    String clientType;

    public SerializableClientEndPoint() {
    }

    public SerializableClientEndPoint(Client client) {
        this.uuid = client.getUuid();
        this.address = client.getSocketAddress().getHostName() + ":" + client.getSocketAddress().getPort();
        this.clientType = client.getClientType().toString();
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getClientType() {
        return clientType;
    }

    public void setClientType(String clientType) {
        this.clientType = clientType;
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        root.add("uuid", uuid);
        root.add("address", address);
        root.add("clientType", clientType);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        uuid = getString(json, "uuid");
        address = getString(json, "address");
        clientType = getString(json, "clientType");
    }
}
