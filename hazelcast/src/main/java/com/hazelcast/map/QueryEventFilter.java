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

package com.hazelcast.map;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryEntry;

import java.io.IOException;
import java.util.Map;

public class QueryEventFilter extends EntryEventFilter  {

    Predicate predicate = null;

    public QueryEventFilter(boolean includeValue, Data key, Predicate predicate) {
        super(includeValue, key);
        this.predicate = predicate;
    }

    public QueryEventFilter() {
        super();
    }

    public Object getPredicate() {
        return predicate;
    }

    public boolean eval(Object arg) {
        final QueryEntry entry = (QueryEntry) arg;
        final Data keyData = entry.getKeyData();
        return (key == null || key.equals(keyData)) && predicate.apply((Map.Entry)arg);
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
        if (!predicate.equals(that.predicate)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return predicate.hashCode();
    }
}
