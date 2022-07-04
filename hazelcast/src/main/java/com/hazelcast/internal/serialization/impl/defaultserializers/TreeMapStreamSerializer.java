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

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeMap;

/**
 * The {@link TreeMap} serializer
 */
public class TreeMapStreamSerializer<K, V> extends AbstractMapStreamSerializer<TreeMap<K, V>> {
    @Override
    public int getTypeId() {
        return SerializationConstants.JAVA_DEFAULT_TYPE_TREE_MAP;
    }

    @Override
    public void write(ObjectDataOutput out, TreeMap<K, V> map) throws IOException {
        out.writeObject(((TreeMap<K, V>) map).comparator());

        super.write(out, map);
    }

    @Override
    public TreeMap<K, V> read(ObjectDataInput in) throws IOException {
        Comparator<? super K> comparator = in.readObject();

        TreeMap<K, V> map = new TreeMap<>(comparator);

        int size = in.readInt();

        return deserializeEntries(in, size, map);
    }
}
