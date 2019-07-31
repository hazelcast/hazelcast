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
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Collection;

import static com.hazelcast.nio.Bits.NULL_ARRAY_LENGTH;

/**
 * The {@link Collection} serializer
 */
public abstract class AbstractCollectionStreamSerializer implements StreamSerializer<Collection> {

    @Override
    public void write(ObjectDataOutput out, Collection collection) throws IOException {
        int size = collection == null ? NULL_ARRAY_LENGTH : collection.size();
        out.writeInt(size);
        if (size > 0) {
            beforeSerializeEntries(out, collection);
            Iterator iterator = collection.iterator();
            while (iterator.hasNext()) {
                out.writeObject(iterator.next());
            }
        }
    }

    @Override
    public Collection read(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        Collection collection = null;
        if (size > NULL_ARRAY_LENGTH) {
            collection = createCollection(size, in);
            for (int i = 0; i < size; i++) {
                collection.add(in.readObject());
            }
        }
        return collection;
    }

    @Override
    public void destroy() {
    }

    protected abstract Collection createCollection(int size, ObjectDataInput in)
            throws IOException;

    protected void beforeSerializeEntries(ObjectDataOutput out, Collection collection)
            throws IOException {
    }
}
