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
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.IndexImpl;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.MultiResultCollector;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Equal Predicate
 */
public class EqualPredicate extends AbstractPredicate implements NegatablePredicate {
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
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Index index = getIndex(queryContext);
        return index.getRecords(value);
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        Object entryValue = readAttribute(mapEntry);
        if (entryValue instanceof MultiResultCollector) {
            return applyForMultiResult(mapEntry, (MultiResultCollector) entryValue);
        }
        return applyForSingleValue(mapEntry, (Comparable) entryValue);
    }

    private boolean applyForMultiResult(Map.Entry mapEntry, MultiResultCollector result) {
        List<Object> results = result.getResults();
        for (Object o : results) {
            Comparable entryValue = (Comparable) convertAttribute(o);
            boolean applied = applyForSingleValue(mapEntry, entryValue);
            if (applied) {
                return true;
            }
        }
        return false;
    }

    private boolean applyForSingleValue(Map.Entry mapEntry, Comparable entryValue) {
        if (entryValue == null) {
            return value == null || value == IndexImpl.NULL;
        }
        value = convert(mapEntry, entryValue, value);
        return entryValue.equals(value);
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

    @Override
    public Predicate negate() {
        return new NotEqualPredicate(attribute, value);
    }
}
