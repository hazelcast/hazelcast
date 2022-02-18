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

package com.hazelcast.internal.serialization.impl.defaultserializers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.util.Collection;

/**
 * The {@link Collection} serializer
 */
abstract class AbstractCollectionStreamSerializer<CollectionType extends Collection<?>>
        implements StreamSerializer<CollectionType> {

    @Override
    public void write(ObjectDataOutput out, CollectionType collection) throws IOException {
        int size = collection.size();
        out.writeInt(size);
        for (Object o : collection) {
            out.writeObject(o);
        }
    }

    CollectionType deserializeEntries(ObjectDataInput in, int size, CollectionType collection)
            throws IOException {
        deserializeEntriesInto(in, size, collection);
        return collection;
    }

    void deserializeEntriesInto(ObjectDataInput in, int size, Collection<?> collection)
            throws IOException {
        for (int i = 0; i < size; i++) {
            collection.add(in.readObject());
        }
    }

    @Override
    public void destroy() {
    }

}
