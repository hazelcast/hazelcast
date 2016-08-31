package com.hazelcast.cardinality.hyperloglog.operations;

import com.hazelcast.cardinality.hyperloglog.HyperLogLogContainer;
import com.hazelcast.cardinality.hyperloglog.HyperLogLogDataSerializerHook;
import com.hazelcast.cardinality.hyperloglog.HyperLogLogService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

public abstract class AbstractHyperLogLogOperation extends Operation
        implements PartitionAwareOperation, IdentifiedDataSerializable {

    protected String name;

    public AbstractHyperLogLogOperation() {}

    public AbstractHyperLogLogOperation(String name) {
        this.name = name;
    }

    @Override
    public String getServiceName() {
        return HyperLogLogService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return HyperLogLogDataSerializerHook.F_ID;
    }

    public HyperLogLogContainer getHyperLogLogContainer() {
        HyperLogLogService service = getService();
        return service.getHyperLogLogContainer(name);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        this.name = in.readUTF();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", name=").append(name);
    }
}
