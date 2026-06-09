/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.storage;

import com.hazelcast.internal.serialization.Data;

import java.util.function.Consumer;

public interface VectorKeysEntry {

    /**
     * Adds the specified key to the collection.
     *
     * @param key the key to be added
     * @return a VectorKeysEntry instance with an added key.
     */
    VectorKeysEntry addKey(Data key);

    /**
     * Removes the specified key from the collection.
     *
     * @param key the key to be removed
     */
    void deleteKey(Data key);

    /**
     * Checks if the collection of keys is empty.
     *
     * @return {@code true} if the collection of keys is empty, {@code false} otherwise.
     */
    boolean isEmptyKeys();

    /**
     * Performs the specified action for each key in this collection,
     * stopping when the limit is reached.
     *
     * @param limit  the maximum number of keys to process
     * @param action the action to be performed for each key
     * @return number of keys processed
     */
    int forEachKeyWithLimit(int limit, Consumer<Data> action);
}
