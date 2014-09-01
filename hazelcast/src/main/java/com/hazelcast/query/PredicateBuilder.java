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

import com.hazelcast.query.impl.predicate.AndPredicate;
import com.hazelcast.query.impl.predicate.OrPredicate;

import java.util.ArrayList;
import java.util.List;

/**
 * This class provides functionality to build predicate.
 */
public class PredicateBuilder {

    List<Predicate> lsPredicates = new ArrayList<Predicate>();

    private String attribute;

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public EntryObject getEntryObject() {
        return new EntryObject(this);
    }

    public PredicateBuilder and(PredicateBuilder predicateBuilder) {
        return and(predicateBuilder.build());
    }

    public PredicateBuilder or(PredicateBuilder predicateBuilder) {
        return or(predicateBuilder.build());
    }

    public PredicateBuilder and(Predicate predicate) {
        if (predicate != PredicateBuilder.this.build()) {
            throw new QueryException("Illegal and statement expected: "
                    + PredicateBuilder.class.getSimpleName() + ", found: "
                    + ((predicate == null) ? "null" : predicate.getClass().getSimpleName()));
        }
        int index = lsPredicates.size() - 2;
        Predicate first = lsPredicates.remove(index);
        Predicate second = lsPredicates.remove(index);
        lsPredicates.add(new AndPredicate(first, second));
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
        lsPredicates.add(new OrPredicate(first, second));
        return this;
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

    // should we create a copy of inline predicate?
    public Predicate build() {
        return lsPredicates.size() > 0 ? lsPredicates.get(0) : null;
    }
}
