/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
