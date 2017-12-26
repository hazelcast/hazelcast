package com.hazelcast.dataseries.impl.entryprocessor;

import com.hazelcast.dataseries.EntryProcessorRecipe;
import com.hazelcast.dataseries.impl.DataSeriesDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public class PrepareEntryProcessorOperationFactory implements OperationFactory {
    private String name;
    private String preparationId;
    private EntryProcessorRecipe recipe;

    public PrepareEntryProcessorOperationFactory() {
    }

    public PrepareEntryProcessorOperationFactory(String name,
                                                 String preparationId,
                                                 EntryProcessorRecipe recipe) {
        this.name = name;
        this.preparationId = preparationId;
        this.recipe = recipe;
    }

    @Override
    public Operation createOperation() {
        return new PrepareEntryProcessorOperation(name, preparationId, recipe);
    }

    @Override
    public int getFactoryId() {
        return DataSeriesDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DataSeriesDataSerializerHook.PREPARE_ENTRY_PROCESSOR_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(preparationId);
        out.writeObject(recipe);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        preparationId = in.readUTF();
        recipe = in.readObject();
    }
}
