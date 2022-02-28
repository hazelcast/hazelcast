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

import static com.hazelcast.internal.util.MapUtil.createHashMap;

public class DuplicateDetectingMultiResult extends AbstractSet<QueryableEntry> implements MultiResultSet {
    private Map<Data, QueryableEntry> records;

    @Override
    public void addResultSet(Map<Data, QueryableEntry> resultSet) {
        if (records == null) {
            records = createHashMap(resultSet.size());
        }

        for (Map.Entry<Data, QueryableEntry> entry : resultSet.entrySet()) {
            Data key = entry.getKey();
            QueryableEntry value = entry.getValue();
            records.put(key, value);
        }
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
            return Collections.<QueryableEntry>emptyList().iterator();
        }
        return records.values().iterator();
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
