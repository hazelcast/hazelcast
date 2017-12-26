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

package com.hazelcast.dictionary.impl.operations;

import com.hazelcast.dictionary.AggregationRecipe;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.dictionary.impl.DictionaryDataSerializerHook.PREPARE_AGGREGATION_OPERATION;

public class PrepareAggregationOperation extends DictionaryOperation {

    private AggregationRecipe aggregationRecipe;
    private String preparationId;

    public PrepareAggregationOperation() {
    }

    public PrepareAggregationOperation(String name, String preparationId, AggregationRecipe aggregationRecipe) {
        super(name);
        this.aggregationRecipe = aggregationRecipe;
        this.preparationId = preparationId;
    }

    @Override
    public int getId() {
        return PREPARE_AGGREGATION_OPERATION;
    }

    @Override
    public void run() throws Exception {
        partition.prepareAggregation(preparationId, aggregationRecipe);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(preparationId);
        out.writeObject(aggregationRecipe);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        preparationId = in.readUTF();
        aggregationRecipe = in.readObject();
    }
}
