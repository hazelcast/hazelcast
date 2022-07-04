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

import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

/**
 * The {@link TreeSet} serializer
 */
public class TreeSetStreamSerializer<E> extends AbstractCollectionStreamSerializer<Set<E>> {

    @Override
    public int getTypeId() {
        return SerializationConstants.JAVA_DEFAULT_TYPE_TREE_SET;
    }

    @SuppressFBWarnings(value = "BC_BAD_CAST_TO_CONCRETE_COLLECTION",
            justification = "The map is guaranteed to be of type TreeSet when this method is called.")
    @Override
    public void write(ObjectDataOutput out, Set<E> collection) throws IOException {
        out.writeObject(((TreeSet<E>) collection).comparator());

        super.write(out, collection);
    }

    @Override
    public Set<E> read(ObjectDataInput in) throws IOException {
        Comparator<E> comparator = in.readObject();

        Set<E> collection = new TreeSet<>(comparator);

        int size = in.readInt();

        return deserializeEntries(in, size, collection);
    }
}
