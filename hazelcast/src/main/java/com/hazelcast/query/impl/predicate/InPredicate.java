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

package com.hazelcast.query.impl.predicate;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.resultset.FilteredResultSet;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.io.IOException;
import java.util.Arrays;
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
    public boolean in(Predicate predicate) {
        boolean found;
        if (predicate instanceof InPredicate) {
            Comparable[] pValues = ((InPredicate) predicate).getValues();
            for (Comparable value : values) {
                found = false;
                for (Comparable pValue : pValues) {
                    if (value.compareTo(pValue) == 0) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean equals(Object predicate) {
        if (super.equals(predicate) && predicate instanceof InPredicate) {
            InPredicate p = (InPredicate) predicate;
            if (p.values == null || values == null) {
                return values == p.values;
            }
            return Arrays.equals(p.values, values);
        }
        return false;
    }

    public int hashCode() {
        int result = super.hashCode();
        result += values != null ? Arrays.hashCode(values) : 0;
        return result;
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

    public Comparable[] getValues() {
        return values;
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Index index = getIndex(queryContext);
        if (index == null) {
            return null;
        }

        Set<QueryableEntry> records = index.getRecords(values);

        InPredicate notIndexedPredicate = (InPredicate) queryContext.getQueryPlan(true).getMappedNotIndexedPredicate(this);
        if (notIndexedPredicate != null) {

            // we filter already indexed values for better performance.
            Comparable[] allEntries = notIndexedPredicate.getValues();
            int newLength = allEntries.length;
            for (int i = 0; i < allEntries.length; i++) {
                if (exists(allEntries[i])) {
                    allEntries[i] = null;
                    newLength--;
                }
            }
            Comparable[] filtered = new Comparable[newLength];

            for (int lastPosition = 0, i = 0; lastPosition < newLength; i++) {
                Comparable entry = allEntries[i];
                if (entry != null) {
                    filtered[lastPosition] = entry;
                    lastPosition++;
                }
            }
            return new FilteredResultSet(records, new InPredicate(attribute, filtered));
        } else {
            return records;
        }
    }

    private boolean exists(Comparable obj) {
        for (Comparable value : values) {
            if (obj.equals(value)) {
                return true;
            }
        }
        return false;
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
