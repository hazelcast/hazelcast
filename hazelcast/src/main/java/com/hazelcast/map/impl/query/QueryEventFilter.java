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

package com.hazelcast.map.impl.query;

import com.hazelcast.map.impl.EntryEventFilter;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntry;

import java.io.IOException;
import java.util.Map;

/**
 * Event filter which matches map events on a specified entry key and
 * matching a predefined {@link Predicate}.
 */
public class QueryEventFilter extends EntryEventFilter {

    private Predicate predicate;

    public QueryEventFilter() {
    }

    public QueryEventFilter(Data key, Predicate predicate, boolean includeValue) {
        super(key, includeValue);
        this.predicate = predicate;
    }

    public Object getPredicate() {
        return predicate;
    }

    @Override
    public boolean eval(Object arg) {
        QueryableEntry entry = (QueryableEntry) arg;
        Data keyData = entry.getKeyData();
        return (key == null || key.equals(keyData)) && predicate.apply((Map.Entry) arg);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.QUERY_EVENT_FILTER;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(predicate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        predicate = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryEventFilter that = (QueryEventFilter) o;
        if (!super.equals(o)) {
            return false;
        }
        if (!predicate.equals(that.predicate)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + predicate.hashCode();
    }

    @Override
    public String toString() {
        return "QueryEventFilter{"
                + "predicate=" + predicate
                + '}';
    }
}
