/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import java.util.AbstractSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

/**
 *  Multiple result set for Predicates.
 */
public class SingleResultSet extends AbstractSet<QueryableEntry> {
    private final ConcurrentMap<Data, QueryableEntry> records;

    public SingleResultSet(ConcurrentMap<Data, QueryableEntry> records) {
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
    public Iterator<QueryableEntry> iterator() {
        if (records == null) {
            //todo: why are we not returning Collections.EMPTY_SET.iterator?
            return new HashSet<QueryableEntry>().iterator();
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
