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
import com.hazelcast.query.impl.IndexImpl;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.util.ValidationUtil;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Equal Predicate
 */
public class EqualPredicate extends AbstractPredicate {
    protected Comparable value;

    public EqualPredicate() {

    }

    public EqualPredicate(String attribute) {
        super(attribute);
    }

    public EqualPredicate(String attribute, Comparable value) {
        super(attribute);
        this.value = value;
    }

    @Override
    public boolean equals(Object predicate) {
        if (super.equals(predicate) && predicate instanceof EqualPredicate) {
            EqualPredicate p = (EqualPredicate) predicate;
            return ValidationUtil.equalOrNull(p.value, value);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }


    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Index index = getIndex(queryContext);
        Set<QueryableEntry> records = index.getRecords(value);

        Predicate notIndexedPredicate = queryContext.getQueryPlan(true).getMappedNotIndexedPredicate(this);
        if (notIndexedPredicate != null) {
            return new FilteredResultSet(records, notIndexedPredicate);
        } else {
            return records;
        }
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        Comparable entryValue = readAttribute(mapEntry);
        if (entryValue == null) {
            return value == null || value == IndexImpl.NULL;
        }
        value = convert(mapEntry, entryValue, value);
        return entryValue.equals(value);
    }

    @Override
    public boolean in(Predicate predicate) {
        return equals(predicate);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        value = in.readObject();
    }

    @Override
    public String toString() {
        return attribute + "=" + value;
    }
}
