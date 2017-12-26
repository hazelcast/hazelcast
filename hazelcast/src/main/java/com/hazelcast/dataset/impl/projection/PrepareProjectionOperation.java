package com.hazelcast.dataset.impl.projection;

import com.hazelcast.dataset.ProjectionRecipe;
import com.hazelcast.dataset.impl.operations.DataSetOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.dataset.impl.DataSetDataSerializerHook.PREPARE_PROJECTION_OPERATION;

public class PrepareProjectionOperation extends DataSetOperation {

    private ProjectionRecipe projectionRecipe;
    private String preparationId;

    public PrepareProjectionOperation() {
    }

    public PrepareProjectionOperation(String name, String preparationId, ProjectionRecipe projectionRecipe) {
        super(name);
        this.projectionRecipe = projectionRecipe;
        this.preparationId = preparationId;
    }

    @Override
    public int getId() {
        return PREPARE_PROJECTION_OPERATION;
    }

    @Override
    public void run() throws Exception {
        partition.prepareProjection(preparationId, projectionRecipe);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(preparationId);
        out.writeObject(projectionRecipe);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        preparationId = in.readUTF();
        projectionRecipe = in.readObject();
    }
}
