package com.hazelcast.dataset.impl.entryprocessor;

import com.hazelcast.dataset.EntryProcessorRecipe;
import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.dataset.impl.Partition;
import com.hazelcast.dataset.impl.operations.DataSetOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class CompileEpOperation extends DataSetOperation {

    private String compileId;
    private EntryProcessorRecipe recipe;

    public CompileEpOperation() {
    }

    public CompileEpOperation(String name, String compileId, EntryProcessorRecipe recipe) {
        super(name);
        this.compileId = compileId;
        this.recipe = recipe;
    }

    @Override
    public void run() throws Exception {
        partition.compileEntryProcessor(compileId, recipe);
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.COMPILE_ENTRY_PROCESSOR_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeUTF(compileId);
        out.writeObject(recipe);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        compileId = in.readUTF();
        recipe = in.readObject();
    }

}
