/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.util.MapUtil.createHashMap;

public class DuplicateDetectingMultiResult extends AbstractSet<QueryableEntryImpl> implements MultiResultSet {
    private Map<Data, QueryableEntryImpl> records;

    @Override
    public void addResultSet(Map<Data, QueryableEntryImpl> resultSet) {
        if (records == null) {
            records = createHashMap(resultSet.size());
        }

        for (Map.Entry<Data, QueryableEntryImpl> entry : resultSet.entrySet()) {
            Data key = entry.getKey();
            QueryableEntryImpl value = entry.getValue();
            records.put(key, value);
        }
    }

    @Override
    public boolean contains(Object mapEntry) {
        if (records == null) {
            return false;
        }

        Data keyData = ((QueryableEntryImpl) mapEntry).getKeyData();
        return records.containsKey(keyData);
    }

    @Override
    public Iterator<QueryableEntryImpl> iterator() {
        if (records == null) {
            return Collections.<QueryableEntryImpl>emptyList().iterator();
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
