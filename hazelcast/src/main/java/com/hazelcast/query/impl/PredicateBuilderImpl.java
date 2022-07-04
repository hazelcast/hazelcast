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

package com.hazelcast.query.impl;

import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.PredicateBuilder.EntryObject;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.predicates.IndexAwarePredicate;
import com.hazelcast.query.impl.predicates.VisitablePredicate;
import com.hazelcast.query.impl.predicates.Visitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;

@BinaryInterface
public class PredicateBuilderImpl
        implements PredicateBuilder, EntryObject, IndexAwarePredicate, VisitablePredicate, DataSerializable {

    private List<Predicate> lsPredicates = new ArrayList<>();

    private String attribute;

    @Override
    public Predicate accept(Visitor visitor, Indexes indexes) {
        Predicate predicate = lsPredicates.get(0);
        if (predicate instanceof VisitablePredicate) {
            Predicate newPredicate = ((VisitablePredicate) predicate).accept(visitor, indexes);
            if (newPredicate != predicate) {
                PredicateBuilderImpl newPredicateBuilder = new PredicateBuilderImpl();
                newPredicateBuilder.attribute = attribute;
                newPredicateBuilder.lsPredicates.addAll(lsPredicates);
                newPredicateBuilder.lsPredicates.set(0, newPredicate);
                return newPredicateBuilder;
            }
        }

        return this;
    }

    @Override
    public String getAttribute() {
        return attribute;
    }

    @Override
    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        return lsPredicates.get(0).apply(mapEntry);
    }

    @Override
    public EntryObject getEntryObject() {
        return this;
    }

    @Override
    public PredicateBuilder and(Predicate predicate) {
        if (predicate != PredicateBuilderImpl.this) {
            throw new QueryException("Illegal and statement expected: "
                    + PredicateBuilderImpl.class.getSimpleName() + ", found: "
                    + ((predicate == null) ? "null" : predicate.getClass().getSimpleName()));
        }
        int index = lsPredicates.size() - 2;
        Predicate first = lsPredicates.remove(index);
        Predicate second = lsPredicates.remove(index);
        return addPredicate(Predicates.and(first, second));
    }

    @Override
    public PredicateBuilder or(Predicate predicate) {
        if (predicate != PredicateBuilderImpl.this) {
            throw new RuntimeException("Illegal or statement expected: "
                    + PredicateBuilderImpl.class.getSimpleName() + ", found: "
                    + ((predicate == null) ? "null" : predicate.getClass().getSimpleName()));
        }
        int index = lsPredicates.size() - 2;
        Predicate first = lsPredicates.remove(index);
        Predicate second = lsPredicates.remove(index);
        return addPredicate(Predicates.or(first, second));
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
        out.writeString(attribute);
        out.writeInt(lsPredicates.size());
        for (Predicate predicate : lsPredicates) {
            out.writeObject(predicate);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        attribute = in.readString();
        int size = in.readInt();
        lsPredicates = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            addPredicate(in.readObject());
        }
    }

    @Override
    public String toString() {
        return "PredicateBuilder{\n" + (lsPredicates.size() == 0 ? "" : lsPredicates.get(0)) + "\n}";
    }

    @Override
    public EntryObject get(String attribute) {
        if (KEY_ATTRIBUTE_NAME.value().equals(getAttribute())) {
            setAttribute(KEY_ATTRIBUTE_NAME.value() + "." + attribute);
        } else {
            setAttribute(attribute);
        }
        return this;
    }

    @Override
    public EntryObject key() {
        setAttribute(KEY_ATTRIBUTE_NAME.value());
        return this;
    }

    @Override
    public PredicateBuilder is(String attribute) {
        return addPredicate(Predicates.equal(attribute, true));
    }

    @Override
    public PredicateBuilder isNot(String attribute) {
        return addPredicate(Predicates.notEqual(attribute, true));
    }

    @Override
    public PredicateBuilder equal(Comparable value) {
        return addPredicate(Predicates.equal(getAttribute(), value));
    }

    @Override
    public PredicateBuilder notEqual(Comparable value) {
        return addPredicate(Predicates.notEqual(getAttribute(), value));
    }

    @Override
    public PredicateBuilder isNull() {
        return addPredicate(Predicates.equal(getAttribute(), null));
    }

    @Override
    public PredicateBuilder isNotNull() {
        return addPredicate(Predicates.notEqual(getAttribute(), null));
    }

    @Override
    public PredicateBuilder greaterThan(Comparable value) {
        return addPredicate(Predicates.greaterThan(getAttribute(), value));
    }

    @Override
    public PredicateBuilder greaterEqual(Comparable value) {
        return addPredicate(Predicates.greaterEqual(getAttribute(), value));
    }

    @Override
    public PredicateBuilder lessThan(Comparable value) {
        return addPredicate(Predicates.lessThan(getAttribute(), value));
    }

    @Override
    public PredicateBuilder lessEqual(Comparable value) {
        return addPredicate(Predicates.lessEqual(getAttribute(), value));
    }

    @Override
    public PredicateBuilder between(Comparable from, Comparable to) {
        return addPredicate(Predicates.between(getAttribute(), from, to));
    }

    @Override
    public PredicateBuilder in(Comparable... values) {
        return addPredicate(Predicates.in(getAttribute(), values));
    }

    private PredicateBuilder addPredicate(Predicate predicate) {
        lsPredicates.add(predicate);
        return this;
    }

}
