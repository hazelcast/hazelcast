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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * The {@link PriorityBlockingQueue} serializer
 */
public class PriorityBlockingQueueStreamSerializer<E> extends AbstractCollectionStreamSerializer<PriorityBlockingQueue<E>> {

    @Override
    public int getTypeId() {
        return SerializationConstants.JAVA_DEFAULT_TYPE_PRIORITY_BLOCKING_QUEUE;
    }

    @Override
    public void write(ObjectDataOutput out, PriorityBlockingQueue<E> collection) throws IOException {
        out.writeObject(collection.comparator());

        super.write(out, collection);
    }

    @Override
    public PriorityBlockingQueue<E> read(ObjectDataInput in) throws IOException {
        Comparator<E> comparator = in.readObject();

        int size = in.readInt();

        PriorityBlockingQueue<E> collection = new PriorityBlockingQueue<>(size, comparator);

        return deserializeEntries(in, size, collection);
    }
}
