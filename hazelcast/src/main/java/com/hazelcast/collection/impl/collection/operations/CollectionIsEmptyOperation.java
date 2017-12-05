/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.ReadonlyOperation;

public class CollectionIsEmptyOperation extends CollectionOperation implements ReadonlyOperation {

    public CollectionIsEmptyOperation() {
    }

    public CollectionIsEmptyOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        CollectionContainer collectionContainer = getOrCreateContainer();
        response = collectionContainer.size() == 0;
    }

    @Override
    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_IS_EMPTY;
    }
}
