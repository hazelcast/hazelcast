package com.hazelcast.executor.client;

import com.hazelcast.client.PartitionClientRequest;
import com.hazelcast.executor.CallableTaskOperation;
import com.hazelcast.executor.DistributedExecutorService;
import com.hazelcast.executor.ExecutorDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * @mdogan 5/13/13
 */
public final class PartitionCallableRequest extends PartitionClientRequest implements IdentifiedDataSerializable {

    private String name;
    private int partitionId;
    private Callable callable;

    @SuppressWarnings("unchecked")
    @Override
    protected Operation prepareOperation() {
        return new CallableTaskOperation(name, callable);
    }

    @Override
    protected int getPartition() {
        return partitionId;
    }

    @Override
    protected int getReplicaIndex() {
        return 0;
    }

    @Override
    public String getServiceName() {
        return DistributedExecutorService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ExecutorDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ExecutorDataSerializerHook.PARTITION_CALLABLE_REQUEST;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(partitionId);
        out.writeObject(callable);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        partitionId = in.readInt();
        callable = in.readObject();
    }
}
