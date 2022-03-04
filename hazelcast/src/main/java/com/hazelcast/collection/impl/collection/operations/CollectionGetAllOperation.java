/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.collection.operations;

import com.hazelcast.collection.impl.collection.CollectionContainer;
import com.hazelcast.collection.impl.collection.CollectionDataSerializerHook;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.impl.SerializableList;

import java.util.List;

public class CollectionGetAllOperation extends CollectionOperation implements ReadonlyOperation {

    public CollectionGetAllOperation() {
    }

    public CollectionGetAllOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        CollectionContainer collectionContainer = getOrCreateContainer();
        List<Data> all = collectionContainer.getAll();
        response = new SerializableList(all);
    }

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.COLLECTION_GET_ALL;
    }
}
