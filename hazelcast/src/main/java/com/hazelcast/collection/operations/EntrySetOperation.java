/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.operations;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.collection.CollectionProxyType;

/**
 * @ali 1/8/13
 */
public class EntrySetOperation extends CollectionOperation {

    public EntrySetOperation() {
    }

    public EntrySetOperation(String name, CollectionProxyType proxyType) {
        super(name, proxyType);
    }

    public void run() throws Exception {
        CollectionContainer container = getOrCreateContainer();
        response = new EntrySetResponse(container.copyObjects(), getNodeEngine().getSerializationService());
    }
}
