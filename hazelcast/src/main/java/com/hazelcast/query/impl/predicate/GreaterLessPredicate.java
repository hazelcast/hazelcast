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
import com.hazelcast.query.impl.ComparisonType;
import com.hazelcast.query.impl.resultset.FilteredResultSet;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.util.ValidationUtil;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Greater Less Predicate
 */
public class GreaterLessPredicate extends EqualPredicate {
    boolean equal;
    boolean less;

    public GreaterLessPredicate() {
    }

    public GreaterLessPredicate(String attribute, Comparable value, boolean equal, boolean less) {
        super(attribute);

        if (value == null) {
            throw new NullPointerException("Arguments can't be null");
        }

        this.value = value;
        this.equal = equal;
        this.less = less;
    }

    @Override
    public boolean equals(Object predicate) {
        if (super.equals(predicate) && predicate instanceof GreaterLessPredicate) {
            GreaterLessPredicate p = (GreaterLessPredicate) predicate;
            return ValidationUtil.equalOrNull(p.equal, equal) && ValidationUtil.equalOrNull(p.less, less);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (equal ? 1 : 0);
        result = 31 * result + (less ? 1 : 0);
        return result;
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        final Comparable entryValue = readAttribute(mapEntry);
        if (entryValue == null) {
            return false;
        }
        final Comparable attributeValue = convert(mapEntry, entryValue, value);
        final int result = entryValue.compareTo(attributeValue);
        return equal && result == 0 || (less ? (result < 0) : (result > 0));
    }

    @Override
    public boolean in(Predicate predicate) {
        if (predicate instanceof GreaterLessPredicate) {
            GreaterLessPredicate p = (GreaterLessPredicate) predicate;
            // todo: equal may differ even though this.in(predicate).
            if (equal == p.equal && less == p.less) {
                if (less) {
                    int i = value.compareTo(p.value);
                    return equal ? i <= 0 : i < 0;
                } else {
                    int i = value.compareTo(p.value);
                    return equal ? i >= 0 : i > 0;
                }
            }
        }
        return false;
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Index index = getIndex(queryContext);
        final ComparisonType comparisonType;
        if (less) {
            comparisonType = equal ? ComparisonType.LESSER_EQUAL : ComparisonType.LESSER;
        } else {
            comparisonType = equal ? ComparisonType.GREATER_EQUAL : ComparisonType.GREATER;
        }
        Set<QueryableEntry> subRecords = index.getSubRecords(comparisonType, value);

        Predicate notIndexedPredicate = queryContext.getQueryPlan(true).getMappedNotIndexedPredicate(this);
        if (notIndexedPredicate != null) {
            return new FilteredResultSet(subRecords, notIndexedPredicate);
        } else {
            return subRecords;
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        equal = in.readBoolean();
        less = in.readBoolean();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeBoolean(equal);
        out.writeBoolean(less);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(attribute);
        sb.append(less ? "<" : ">");
        if (equal) {
            sb.append("=");
        }
        sb.append(value);
        return sb.toString();
    }
}
