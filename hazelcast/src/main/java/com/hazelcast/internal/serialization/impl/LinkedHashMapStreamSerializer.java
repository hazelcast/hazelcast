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
import com.hazelcast.internal.util.MapUtil;

import java.io.IOException;
import java.util.LinkedHashMap;

/**
 * The {@link LinkedHashMap} serializer
 *
 * Important Note: The ConcurrentHashMap 'loadfactor' is not serialized.
 *
 */
public class LinkedHashMapStreamSerializer<K, V> extends AbstractMapStreamSerializer<LinkedHashMap<K, V>> {
    @Override
    public int getTypeId() {
        return SerializationConstants.JAVA_DEFAULT_TYPE_LINKED_HASH_MAP;
    }

    @Override
    public LinkedHashMap<K, V> read(ObjectDataInput in) throws IOException {
        int size = in.readInt();

        LinkedHashMap<K, V> map = (LinkedHashMap<K, V>) MapUtil.createLinkedHashMap(size);

        return deserializeEntries(in, size, map);
    }
}
