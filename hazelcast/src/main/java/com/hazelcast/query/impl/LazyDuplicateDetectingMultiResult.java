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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.hazelcast.util.MapUtil.createHashMap;

public class LazyDuplicateDetectingMultiResult extends AbstractSet<QueryableEntry> implements LazyMultiResultSet {

    private final List<Supplier<Map<Data, QueryableEntry>>> resultSuppliers
            = new ArrayList<Supplier<Map<Data, QueryableEntry>>>();
    private final Map<Data, QueryableEntry> records = createHashMap(4);
    private boolean initialized;
    private int cachedSize;

    @Override
    public void addResultSetSupplier(Supplier<Map<Data, QueryableEntry>> resultSetSupplier, int resultSetSize) {
        resultSuppliers.add(resultSetSupplier);
        cachedSize += resultSetSize;
    }

    @Override
    public void init() {
        if (!initialized) {
            for (Supplier<Map<Data, QueryableEntry>> orgResult : resultSuppliers) {
                records.putAll(orgResult.get());
            }
            initialized = true;
        }
    }

    @Override
    public Iterator<QueryableEntry> iterator() {
        init();
        if (records == null) {
            return Collections.<QueryableEntry>emptyList().iterator();
        }
        return records.values().iterator();
    }

    @Override
    public boolean contains(Object mapEntry) {
        init();
        Data keyData = ((QueryableEntry) mapEntry).getKeyData();
        return records.containsKey(keyData);
    }

    @Override
    public int size() {
        if (initialized) {
            return records.size();
        } else {
            return cachedSize;
        }
    }
}
