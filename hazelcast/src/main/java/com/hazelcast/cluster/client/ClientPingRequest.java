package com.hazelcast.cluster.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.cluster.ClusterDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * @mdogan 5/17/13
 */
public final class ClientPingRequest extends CallableClientRequest implements IdentifiedDataSerializable {

    @Override
    public Object call() throws Exception {
        return null;
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.PING;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {

    }
}
