package com.hazelcast.management;

import com.hazelcast.core.Client;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import java.io.IOException;

public class SerializableClientEndPoint implements DataSerializable {

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
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(uuid);
        out.writeUTF(address);
        out.writeUTF(clientType);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        uuid = in.readUTF();
        address = in.readUTF();
        clientType = in.readUTF();
    }
}
