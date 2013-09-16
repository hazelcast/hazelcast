/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.client;

import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.collection.CollectionSizeOperation;
import com.hazelcast.spi.Operation;

/**
 * @ali 9/4/13
 */
public class CollectionSizeRequest extends CollectionRequest {

    public CollectionSizeRequest() {
    }

    public CollectionSizeRequest(String name) {
        super(name);
    }

    protected Operation prepareOperation() {
        return new CollectionSizeOperation(name);
    }

    public int getClassId() {
        return CollectionPortableHook.COLLECTION_SIZE;
    }
}
