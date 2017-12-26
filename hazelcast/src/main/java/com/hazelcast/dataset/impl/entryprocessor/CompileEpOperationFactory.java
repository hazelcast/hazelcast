package com.hazelcast.dataset.impl.entryprocessor;

import com.hazelcast.dataset.EntryProcessorRecipe;
import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public class CompileEpOperationFactory implements OperationFactory {
    private String name;
    private String compileId;
    private EntryProcessorRecipe recipe;

    public CompileEpOperationFactory() {
    }

    public CompileEpOperationFactory(String name, String compileId, EntryProcessorRecipe recipe) {
        this.name = name;
        this.compileId = compileId;
        this.recipe = recipe;
    }

    @Override
    public Operation createOperation() {
        return new CompileEpOperation(name, compileId, recipe);
    }

    @Override
    public int getFactoryId() {
        return DataSetDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.COMPILE_ENTRY_PROCESSOR_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(compileId);
        out.writeObject(recipe);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        compileId = in.readUTF();
        recipe = in.readObject();
    }
}
