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

package com.hazelcast.jet.io.spi.tuple;

import com.hazelcast.nio.serialization.DataSerializable;

/**
 * Represents abstract tuple of data;
 *
 * @param <K> - type of keys;
 * @param <V> - type of value;
 */
public interface Tuple<K, V> extends DataSerializable {
    /**
     * Return key-object with specified index;
     *
     * @param index - specified index;
     * @return - corresponding key-object;
     */
    K getKey(int index);

    /**
     * Return size-object with specified index;
     *
     * @param index - specified index;
     * @return - corresponding size-object;
     */
    V getValue(int index);

    /**
     * @return - number of elements in tuple's key part;
     */
    int keySize();

    /**
     * @return - number of elements in tuple's value part;
     */
    int valueSize();

    /**
     * Set key object to the corresponding position of tuple;
     *
     * @param index - index of key-array to be affected;
     * @param key   - value of key;
     */
    void setKey(int index, K key);

    /**
     * Set value object to the corresponding position of tuple;
     *
     * @param index - index of key-array to be affected;
     * @param value - corresponding value to be set;
     */
    void setValue(int index, V value);

    /**
     * @return - clone of key's part array.
     * Data will not be cloned;
     */
    K[] cloneKeys();

    /**
     * @return - clone of value's part array.
     * Data will not be cloned;
     */
    V[] cloneValues();
}
