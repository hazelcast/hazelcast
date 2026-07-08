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

package com.hazelcast.vector.internal.impl.storage;

import com.hazelcast.internal.serialization.Data;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public final class VectorMultiKeysEntry implements VectorKeysEntry {

    // duplicated vectors are expected to be rare, modification of them even rarer.
    // so prefer read performance during searches
    private final Collection<Data> keys = new CopyOnWriteArrayList<>();

    private VectorMultiKeysEntry() {
    }

    @Override
    public VectorKeysEntry addKey(Data key) {
        keys.add(key);
        return this;
    }

    @Override
    public void deleteKey(Data key) {
        keys.remove(key);
    }

    @Override
    public boolean isEmptyKeys() {
        return keys.isEmpty();
    }

    @Override
    public int forEachKeyWithLimit(int limit, Consumer<Data> action) {
        int counter = 0;
        for (var key : keys) {
            if (counter == limit) {
                break;
            }
            action.accept(key);
            counter++;
        }
        return counter;
    }

    public static VectorMultiKeysEntry create() {
        return new VectorMultiKeysEntry();
    }
}
