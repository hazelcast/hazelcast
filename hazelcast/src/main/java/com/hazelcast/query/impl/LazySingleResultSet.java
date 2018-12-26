/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.function.Supplier;

import java.util.AbstractSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Multiple result set for Predicates.
 */
public class LazySingleResultSet extends AbstractSet<QueryableEntry> implements LazyResultSet {
    private final Supplier<Map<Data, QueryableEntry>> recordsSupplier;
    private Map<Data, QueryableEntry> records;
    private boolean initialized;
    private int estimatedSize;

    public LazySingleResultSet(Supplier<Map<Data, QueryableEntry>> recordsSupplier, int resultSetSize) {
        this.recordsSupplier = recordsSupplier;
        this.estimatedSize = resultSetSize;
    }

    @Override
    public void init() {
        if (!initialized) {
            records = recordsSupplier.get();
            initialized = true;
        }
    }

    @Override
    public boolean contains(Object mapEntry) {
        init();
        if (records == null) {
            return false;
        }

        Data keyData = ((QueryableEntry) mapEntry).getKeyData();
        return records.containsKey(keyData);
    }

    @Override
    public Iterator<QueryableEntry> iterator() {
        init();
        if (records == null) {
            return Collections.EMPTY_SET.iterator();
        } else {
            return records.values().iterator();
        }
    }

    @Override
    public int size() {
        if (initialized) {
            return records == null ? 0 : records.size();
        } else {
            return estimatedSize;
        }
    }
}
