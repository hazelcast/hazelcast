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

package com.hazelcast.queue.impl.client;

import com.hazelcast.client.client.RetryableRequest;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.impl.IteratorOperation;
import com.hazelcast.queue.impl.QueuePortableHook;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.PortableCollection;
import com.hazelcast.spi.impl.SerializableCollection;

import java.util.Collection;

/**
 * Provides the request service for {@link com.hazelcast.queue.impl.IteratorOperation}
 */
public class IteratorRequest extends QueueRequest implements RetryableRequest {

    public IteratorRequest() {
    }

    public IteratorRequest(String name) {
        super(name);
    }

    @Override
    protected Operation prepareOperation() {
        return new IteratorOperation(name);
    }

    @Override
    public int getClassId() {
        return QueuePortableHook.ITERATOR;
    }

    @Override
    protected Object filter(Object response) {
        if (response instanceof SerializableCollection) {
            SerializableCollection serializableCollection = (SerializableCollection) response;
            Collection<Data> coll = serializableCollection.getCollection();
            return new PortableCollection(coll);
        }
        return super.filter(response);
    }

    @Override
    public String getMethodName() {
        return "iterator";
    }
}
