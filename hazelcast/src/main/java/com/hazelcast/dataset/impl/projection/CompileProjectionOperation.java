package com.hazelcast.dataset.impl.projection;

import com.hazelcast.dataset.ProjectionRecipe;
import com.hazelcast.dataset.impl.Partition;
import com.hazelcast.dataset.impl.operations.DataSetOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.dataset.impl.DataSetDataSerializerHook.COMPILE_PROJECTION_OPERATION;

public class CompileProjectionOperation extends DataSetOperation {

    public ProjectionRecipe projectionRecipe;
    private String compileId;

    public CompileProjectionOperation() {
    }

    public CompileProjectionOperation(String name, String compileId, ProjectionRecipe projectionRecipe) {
        super(name);
        this.projectionRecipe = projectionRecipe;
        this.compileId = compileId;
    }

    @Override
    public int getId() {
        return COMPILE_PROJECTION_OPERATION;
    }

    @Override
    public void run() throws Exception {
        partition.compileProjection(compileId, projectionRecipe);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(compileId);
        out.writeObject(projectionRecipe);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        compileId = in.readUTF();
        projectionRecipe = in.readObject();
    }
}
