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

import javax.annotation.Nonnull;
import java.util.function.Consumer;

public final class VectorOneKeyEntry implements VectorKeysEntry {

    // If the key is null, it indicates that the entry is empty and ready to be deleted from the map.
    private volatile Data key;

    public VectorOneKeyEntry(@Nonnull Data key) {
        this.key = key;
    }

    @Override
    public VectorKeysEntry addKey(@Nonnull Data key) {
        if (this.key != null) {
            return VectorMultiKeysEntry.create().addKey(this.key).addKey(key);
        }
        this.key = key;
        return this;
    }

    @Override
    public void deleteKey(@Nonnull Data key) {
        if (key.equals(this.key)) {
            this.key = null;
        }
    }

    @Override
    public boolean isEmptyKeys() {
        return key == null;
    }

    @Override
    @SuppressWarnings("InnerAssignment")
    public int forEachKeyWithLimit(int limit, Consumer<Data> action) {
        Data aKey;
        // read key once to avoid race. Key must be volatile to avoid operation reordering/inlining
        // and guarantee visibility.
        if (limit == 0 || (aKey = key) == null) {
            return 0;
        }
        action.accept(aKey);
        return 1;
    }
}
