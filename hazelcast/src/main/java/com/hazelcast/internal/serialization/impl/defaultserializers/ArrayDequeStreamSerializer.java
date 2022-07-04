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

import java.io.IOException;
import java.util.ArrayDeque;

/**
 * The {@link ArrayDeque} serializer
 */
public class ArrayDequeStreamSerializer<E> extends AbstractCollectionStreamSerializer<ArrayDeque<E>> {

    @Override
    public int getTypeId() {
        return SerializationConstants.JAVA_DEFAULT_TYPE_ARRAY_DEQUE;
    }

    @Override
    public ArrayDeque<E> read(ObjectDataInput in) throws IOException {
        int size = in.readInt();

        ArrayDeque<E> collection = new ArrayDeque<>(size);

        return deserializeEntries(in, size, collection);
    }
}
