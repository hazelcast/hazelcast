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

package com.hazelcast.query;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class provides functionality to build predicate.
 */
public class PredicateBuilder implements IndexAwarePredicate, DataSerializable {

    List<Predicate> lsPredicates = new ArrayList<Predicate>();

    private String attribute;

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        return lsPredicates.get(0).apply(mapEntry);
    }

    @Override
    public boolean in(Predicate predicate) {
        for (Predicate lsPredicate : lsPredicates) {
            if (!lsPredicate.in(predicate)) {
                return false;
            }
        }
        return true;
    }

    public EntryObject getEntryObject() {
        return new EntryObject(this);
    }

    public PredicateBuilder and(Predicate predicate) {
        if (predicate != PredicateBuilder.this) {
            throw new QueryException("Illegal and statement expected: "
                    + PredicateBuilder.class.getSimpleName() + ", found: "
                    + ((predicate == null) ? "null" : predicate.getClass().getSimpleName()));
        }
        int index = lsPredicates.size() - 2;
        Predicate first = lsPredicates.remove(index);
        Predicate second = lsPredicates.remove(index);
        lsPredicates.add(Predicates.and(first, second));
        return this;
    }

    public PredicateBuilder or(Predicate predicate) {
        if (predicate != PredicateBuilder.this) {
            throw new RuntimeException("Illegal or statement expected: "
                    + PredicateBuilder.class.getSimpleName() + ", found: "
                    + ((predicate == null) ? "null" : predicate.getClass().getSimpleName()));
        }
        int index = lsPredicates.size() - 2;
        Predicate first = lsPredicates.remove(index);
        Predicate second = lsPredicates.remove(index);
        lsPredicates.add(Predicates.or(first, second));
        return this;
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Predicate p = lsPredicates.get(0);
        if (p instanceof IndexAwarePredicate) {
            return ((IndexAwarePredicate) p).filter(queryContext);
        }
        return null;
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        Predicate p = lsPredicates.get(0);
        if (p instanceof IndexAwarePredicate) {
            return ((IndexAwarePredicate) p).isIndexed(queryContext);
        }
        return false;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(attribute);
        out.writeInt(lsPredicates.size());
        for (Predicate predicate : lsPredicates) {
            out.writeObject(predicate);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        attribute = in.readUTF();
        int size = in.readInt();
        lsPredicates = new ArrayList<Predicate>(size);
        for (int i = 0; i < size; i++) {
            lsPredicates.add((Predicate) in.readObject());
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("PredicateBuilder");
        sb.append("{\n");
        sb.append(lsPredicates.size() == 0 ? "" : lsPredicates.get(0));
        sb.append("\n}");
        return sb.toString();
    }
}
