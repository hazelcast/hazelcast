/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.ops;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.impl.VectorCollectionSerializerHook;

import java.io.IOException;

public class SearchOperationsFactory implements OperationFactory {
    private String vectorCollectionName;
    private VectorValues vectors;
    private SearchOptions searchOptions;

    public SearchOperationsFactory() {
    }

    public SearchOperationsFactory(String vectorCollectionName, VectorValues vectors, SearchOptions searchOptions) {
        this.vectorCollectionName = vectorCollectionName;
        this.vectors = vectors;
        this.searchOptions = searchOptions;
    }

    @Override
    public int getFactoryId() {
        return VectorCollectionSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.SEARCH_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(vectorCollectionName);
        out.writeObject(vectors);
        out.writeObject(searchOptions);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.vectorCollectionName = in.readString();
        this.vectors = in.readObject();
        this.searchOptions = in.readObject();
    }

    @Override
    public Operation createOperation() {
        return new SearchOperation(vectorCollectionName, vectors, searchOptions);
    }
}
