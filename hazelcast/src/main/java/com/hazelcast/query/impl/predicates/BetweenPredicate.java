/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Between Predicate
 */
@BinaryInterface
public class BetweenPredicate extends AbstractIndexAwarePredicate {

    Comparable to;
    Comparable from;

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

    @Override
    protected boolean applyForSingleAttributeValue(Map.Entry entry, Comparable attributeValue) {
        if (attributeValue == null) {
            return false;
        }
        Comparable fromConvertedValue = convert(entry, attributeValue, from);
        Comparable toConvertedValue = convert(entry, attributeValue, to);
        if (fromConvertedValue == null || toConvertedValue == null) {
            return false;
        }
        return attributeValue.compareTo(fromConvertedValue) >= 0 && attributeValue.compareTo(toConvertedValue) <= 0;
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Index index = getIndex(queryContext);
        return index.getSubRecordsBetween(from, to);
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
        return attributeName + " BETWEEN " + from + " AND " + to;
    }

    @Override
    public int getId() {
        return PredicateDataSerializerHook.BETWEEN_PREDICATE;
    }
}
