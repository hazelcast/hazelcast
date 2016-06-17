/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.io.tuple;

import com.hazelcast.nio.serialization.DataSerializable;

/**
 * A record consisting (conceptually) of an array of keys and an array of values.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface Tuple<K, V> extends DataSerializable {

    /**
     * Returns the key at the given index. May return {@code null}. Allowed range for the index
     * is {@code 0 .. (keySize - 1)}.
     */
    K getKey(int index);

    /**
     * Returns the value at the given index. May return {@code null}. Allowed range for the index
     * is {@code 0 .. (valueSize - 1)}.
     */
    V getValue(int index);

    /** Returns the size of the key array. */
    int keySize();

    /** Returns the size of the value array. */
    int valueSize();

    /** Puts the given object into the given key slot. */
    void setKey(int index, K key);

    /** Puts the given object into the given value slot. */
    void setValue(int index, V value);

    /** Returns a shallow clone of the key array. */
    K[] cloneKeys();

    /** Returns a shallow clone of the value array. */
    V[] cloneValues();
}
