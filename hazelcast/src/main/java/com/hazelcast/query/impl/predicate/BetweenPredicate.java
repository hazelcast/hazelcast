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
import com.hazelcast.util.ValidationUtil;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Between Predicate
 */
public class BetweenPredicate extends AbstractPredicate {
    private Comparable to;
    private Comparable from;

    public BetweenPredicate() {
    }

    public BetweenPredicate(String first, Comparable from, Comparable to) {
        super(first);
        if (from == null || to == null) {
            throw new NullPointerException("Arguments can't be null");
        }
        this.from = from;
        this.to = to;
    }

    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (to != null ? to.hashCode() : 0);
        result = 31 * result + (from != null ? from.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object predicate) {
        if (super.equals(predicate) && predicate instanceof BetweenPredicate) {
            BetweenPredicate p = (BetweenPredicate) predicate;
            return ValidationUtil.equalOrNull(p.to, to) && ValidationUtil.equalOrNull(p.from, from);
        }
        return false;
    }

    @Override
    public boolean apply(Map.Entry entry) {
        Comparable entryValue = readAttribute(entry);
        if (entryValue == null) {
            return false;
        }
        Comparable fromConvertedValue = convert(entry, entryValue, from);
        Comparable toConvertedValue = convert(entry, entryValue, to);
        if (fromConvertedValue == null || toConvertedValue == null) {
            return false;
        }
        return entryValue.compareTo(fromConvertedValue) >= 0 && entryValue.compareTo(toConvertedValue) <= 0;
    }

    @Override
    public boolean in(Predicate predicate) {
        if (super.equals(predicate) && predicate instanceof BetweenPredicate) {
            BetweenPredicate p = (BetweenPredicate) predicate;
            if (to == null || p.to == null || from == null || p.from == null) {
                return false;
            }
            return to.compareTo(p.to) <= 0 && from.compareTo(p.from) >= 0;
        }
        return false;
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Index index = getIndex(queryContext);

        Set<QueryableEntry> subRecordsBetween = index.getSubRecordsBetween(from, to);
        Predicate notIndexedPredicate = queryContext.getQueryPlan(true).getMappedNotIndexedPredicate(this);
        if (notIndexedPredicate != null) {
            return new FilteredResultSet(subRecordsBetween, notIndexedPredicate);
        } else {
            return subRecordsBetween;
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(to);
        out.writeObject(from);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        to = in.readObject();
        from = in.readObject();
    }

    @Override
    public String toString() {
        return attribute + " BETWEEN " + from + " AND " + to;
    }
}
