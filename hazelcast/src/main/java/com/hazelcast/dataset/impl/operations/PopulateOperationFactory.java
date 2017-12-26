package com.hazelcast.dataset.impl.operations;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public class PopulateOperationFactory implements OperationFactory {

    private String dstName;
    private String srcName;

    public PopulateOperationFactory() {
    }

    public PopulateOperationFactory(String dstName, String srcName) {
        this.dstName = dstName;
        this.srcName = srcName;
    }

    @Override
    public Operation createOperation() {
        return new PopulateOperation(dstName, srcName);
    }

    @Override
    public int getFactoryId() {
        return DataSetDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.POPULATE_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(dstName);
        out.writeUTF(srcName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dstName = in.readUTF();
        srcName = in.readUTF();
    }
}