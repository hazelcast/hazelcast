package com.hazelcast.dataseries.impl.projection;

import com.hazelcast.dataseries.ProjectionRecipe;
import com.hazelcast.dataseries.impl.DataSeriesDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public class PrepareProjectionOperationFactory implements OperationFactory {

    private String preparationId;
    private String name;
    private ProjectionRecipe projectionRecipe;

    public PrepareProjectionOperationFactory() {
    }

    public PrepareProjectionOperationFactory(String name,
                                             String preparationId,
                                             ProjectionRecipe projectionRecipe) {
        this.name = name;
        this.preparationId = preparationId;
        this.projectionRecipe = projectionRecipe;
    }

    @Override
    public Operation createOperation() {
        return new PrepareProjectionOperation(name, preparationId, projectionRecipe);
    }

    @Override
    public int getFactoryId() {
        return DataSeriesDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DataSeriesDataSerializerHook.PREPARE_PROJECTION_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(preparationId);
        out.writeObject(projectionRecipe);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        preparationId = in.readUTF();
        projectionRecipe = in.readObject();
    }
}