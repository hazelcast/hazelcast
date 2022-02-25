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

package com.hazelcast.query.impl;

import com.hazelcast.internal.serialization.Data;

import java.util.AbstractSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Multiple result set for Predicates.
 */
public class SingleResultSet extends AbstractSet<QueryableEntry> {
    private final Map<Data, QueryableEntry> records;

    public SingleResultSet(Map<Data, QueryableEntry> records) {
        this.records = records;
    }

    @Override
    public boolean contains(Object mapEntry) {
        if (records == null) {
            return false;
        }

        Data keyData = ((QueryableEntry) mapEntry).getKeyData();
        return records.containsKey(keyData);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<QueryableEntry> iterator() {
        if (records == null) {
            return Collections.EMPTY_SET.iterator();
        } else {
            return records.values().iterator();
        }
    }

    @Override
    public int size() {
        if (records == null) {
            return 0;
        } else {
            return records.size();
        }
    }
}
