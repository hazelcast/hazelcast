package com.hazelcast.dataseries.impl.projection;

import com.hazelcast.dataseries.ProjectionRecipe;
import com.hazelcast.dataseries.impl.operations.DataSeriesOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.dataseries.impl.DataSeriesDataSerializerHook.PREPARE_PROJECTION_OPERATION;

public class PrepareProjectionOperation extends DataSeriesOperation {

    private ProjectionRecipe projectionRecipe;
    private String preparationId;

    public PrepareProjectionOperation() {
    }

    public PrepareProjectionOperation(String name,
                                      String preparationId,
                                      ProjectionRecipe projectionRecipe) {
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
