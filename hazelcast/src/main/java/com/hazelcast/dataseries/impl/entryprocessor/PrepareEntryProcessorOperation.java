package com.hazelcast.dataseries.impl.entryprocessor;

import com.hazelcast.dataseries.EntryProcessorRecipe;
import com.hazelcast.dataseries.impl.DataSeriesDataSerializerHook;
import com.hazelcast.dataseries.impl.operations.DataSeriesOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class PrepareEntryProcessorOperation extends DataSeriesOperation {

    private String preparationId;
    private EntryProcessorRecipe recipe;

    public PrepareEntryProcessorOperation() {
    }

    public PrepareEntryProcessorOperation(String name, String preparationId, EntryProcessorRecipe recipe) {
        super(name);
        this.preparationId = preparationId;
        this.recipe = recipe;
    }

    @Override
    public void run() throws Exception {
        partition.prepareEntryProcessor(preparationId, recipe);
    }

    @Override
    public int getId() {
        return DataSeriesDataSerializerHook.PREPARE_ENTRY_PROCESSOR_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeUTF(preparationId);
        out.writeObject(recipe);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        preparationId = in.readUTF();
        recipe = in.readObject();
    }

}
