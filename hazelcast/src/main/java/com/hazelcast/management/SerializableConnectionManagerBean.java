package com.hazelcast.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.nio.ConnectionManager;

import static com.hazelcast.util.JsonUtil.getInt;

/**
 * A Serializable DTO for {@link com.hazelcast.jmx.ConnectionManagerMBean}.
 */
public class SerializableConnectionManagerBean implements JsonSerializable {

    private int clientConnectionCount;
    private int activeConnectionCount;
    private int connectionCount;

    public SerializableConnectionManagerBean() {
    }

    public SerializableConnectionManagerBean(ConnectionManager cm) {
        this.clientConnectionCount = cm.getCurrentClientConnections();
        this.activeConnectionCount = cm.getActiveConnectionCount();
        this.connectionCount = cm.getConnectionCount();
    }

    public int getClientConnectionCount() {
        return clientConnectionCount;
    }

    public void setClientConnectionCount(int clientConnectionCount) {
        this.clientConnectionCount = clientConnectionCount;
    }

    public int getActiveConnectionCount() {
        return activeConnectionCount;
    }

    public void setActiveConnectionCount(int activeConnectionCount) {
        this.activeConnectionCount = activeConnectionCount;
    }

    public int getConnectionCount() {
        return connectionCount;
    }

    public void setConnectionCount(int connectionCount) {
        this.connectionCount = connectionCount;
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        root.add("clientConnectionCount", clientConnectionCount);
        root.add("activeConnectionCount", activeConnectionCount);
        root.add("connectionCount", connectionCount);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        clientConnectionCount = getInt(json, "clientConnectionCount", -1);
        activeConnectionCount = getInt(json, "activeConnectionCount", -1);
        connectionCount = getInt(json, "connectionCount", -1);
    }
}
