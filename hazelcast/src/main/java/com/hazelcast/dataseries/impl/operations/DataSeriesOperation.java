package com.hazelcast.dataseries.impl.operations;

import com.hazelcast.dataseries.impl.DataSeriesContainer;
import com.hazelcast.dataseries.impl.DataSeriesDataSerializerHook;
import com.hazelcast.dataseries.impl.DataSeriesService;
import com.hazelcast.dataseries.impl.Partition;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.NamedOperation;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public abstract class DataSeriesOperation extends Operation
        implements IdentifiedDataSerializable, NamedOperation {

    private String name;
    protected DataSeriesService dataSeriesService;
    protected DataSeriesContainer container;
    protected Partition partition;

    public DataSeriesOperation() {
    }

    public DataSeriesOperation(String name) {
        this.name = name;
    }

    @Override
    public void beforeRun() throws Exception {
        super.beforeRun();
        dataSeriesService = getService();
        container = dataSeriesService.getDataSeriesContainer(name);
        partition = container.getPartition(getPartitionId());
        partition.deleteRetiredSegments();
    }

    @Override
    public String getServiceName() {
        return DataSeriesService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getFactoryId() {
        return DataSeriesDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", name=").append(name);
    }
}
