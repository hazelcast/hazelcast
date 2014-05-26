package com.hazelcast.management;

import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import java.io.IOException;

/**
 * A Serializable DTO for {@link com.hazelcast.jmx.ConnectionManagerMBean}.
 */
public class SerializableConnectionManagerBean implements DataSerializable {

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
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(clientConnectionCount);
        out.writeInt(activeConnectionCount);
        out.writeInt(connectionCount);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        clientConnectionCount = in.readInt();
        activeConnectionCount = in.readInt();
        connectionCount = in.readInt();
    }
}
