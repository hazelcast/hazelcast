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
import com.hazelcast.internal.util.MapUtil;

import java.io.IOException;
import java.util.HashMap;

/**
 * The {@link HashMap} serializer
 *
 * Important Note: The HashMap 'loadfactor' is not serialized.
 *
 */
public class HashMapStreamSerializer<K, V> extends AbstractMapStreamSerializer<HashMap<K, V>> {
    @Override
    public int getTypeId() {
        return SerializationConstants.JAVA_DEFAULT_TYPE_HASH_MAP;
    }

    @Override
    public HashMap<K, V> read(ObjectDataInput in) throws IOException {
        int size = in.readInt();

        HashMap<K, V> map = (HashMap<K, V>) MapUtil.createHashMap(size);

        return deserializeEntries(in, size, map);
    }
}
