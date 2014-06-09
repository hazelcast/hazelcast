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
