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

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * The {@link CopyOnWriteArraySet} serializer
 */
public class CopyOnWriteArraySetStreamSerializer<E> extends AbstractCollectionStreamSerializer<CopyOnWriteArraySet<E>> {

    @Override
    public int getTypeId() {
        return SerializationConstants.JAVA_DEFAULT_TYPE_COPY_ON_WRITE_ARRAY_SET;
    }

    @Override
    public CopyOnWriteArraySet<E> read(ObjectDataInput in) throws IOException {
        int size = in.readInt();

        CopyOnWriteArraySet<E> collection = new CopyOnWriteArraySet<>();

        return deserializeEntries(in, size, collection);
    }
}
