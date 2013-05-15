package com.hazelcast.partition.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.PartitionDataSerializerHook;
import com.hazelcast.partition.PartitionServiceImpl;

import java.io.IOException;

/**
 * @mdogan 5/13/13
 */
public final class GetPartitionRequest extends CallableClientRequest implements IdentifiedDataSerializable {

    private int partitionId;

    @Override
    public Object call() throws Exception {
        return null;
    }

    @Override
    public String getServiceName() {
        return PartitionServiceImpl.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.GET_PARTITION;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(partitionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        partitionId = in.readInt();
    }
}
