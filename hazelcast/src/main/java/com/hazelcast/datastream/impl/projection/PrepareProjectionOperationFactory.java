/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.datastream.impl.projection;

import com.hazelcast.datastream.ProjectionRecipe;
import com.hazelcast.datastream.impl.DSDataSerializerHook;
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
        return DSDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DSDataSerializerHook.PREPARE_PROJECTION_OPERATION_FACTORY;
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