/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * In Predicate
 */
public class InPredicate extends AbstractPredicate {
    private Comparable[] values;
    private volatile Set<Comparable> convertedInValues;

    public InPredicate() {
    }

    public InPredicate(String attribute, Comparable... values) {
        super(attribute);

        if (values == null) {
            throw new NullPointerException("Array can't be null");
        }
        this.values = values;
    }

    @Override
    public boolean apply(Map.Entry entry) {
        Comparable entryValue = readAttribute(entry);
        if (entryValue == null) {
            return false;
        }
        Set<Comparable> set = convertedInValues;
        if (set == null) {
            set = new HashSet<Comparable>(values.length);
            for (Comparable value : values) {
                set.add(convert(entry, entryValue, value));
            }
            convertedInValues = set;
        }
        return set.contains(entryValue);
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Index index = getIndex(queryContext);
        if (index != null) {
            return index.getRecords(values);
        } else {
            return null;
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(values.length);
        for (Object value : values) {
            out.writeObject(value);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        int len = in.readInt();
        values = new Comparable[len];
        for (int i = 0; i < len; i++) {
            values[i] = in.readObject();
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(attribute);
        sb.append(" IN (");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(values[i]);
        }
        sb.append(")");
        return sb.toString();
    }
}
