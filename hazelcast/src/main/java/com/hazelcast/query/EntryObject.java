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

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;

/**
 * This class provides functionality related to build predicate.
 */
public class EntryObject {
    PredicateBuilder qb;

    public EntryObject(PredicateBuilder qb) {
        this.qb = qb;
    }

    public EntryObject get(String attribute) {
        if (KEY_ATTRIBUTE_NAME.equals(qb.getAttribute())) {
            qb.setAttribute(KEY_ATTRIBUTE_NAME + "#" + attribute);
        } else {
            qb.setAttribute(attribute);
        }
        return this;
    }

    public EntryObject key() {
        qb.setAttribute(KEY_ATTRIBUTE_NAME);
        return this;
    }

    public PredicateBuilder is(String attribute) {
        return addPredicate(Predicates.equal(attribute, true));
    }

    public PredicateBuilder isNot(String attribute) {
        return addPredicate(Predicates.notEqual(attribute, true));
    }

    public PredicateBuilder equal(Comparable value) {
        return addPredicate(Predicates.equal(qb.getAttribute(), value));
    }

    public PredicateBuilder notEqual(Comparable value) {
        return addPredicate(Predicates.notEqual(qb.getAttribute(), value));
    }

    public PredicateBuilder isNull() {
        return addPredicate(Predicates.equal(qb.getAttribute(), null));
    }

    public PredicateBuilder isNotNull() {
        return addPredicate(Predicates.notEqual(qb.getAttribute(), null));
    }

    public PredicateBuilder greaterThan(Comparable value) {
        return addPredicate(Predicates.greaterThan(qb.getAttribute(), value));
    }

    public PredicateBuilder greaterEqual(Comparable value) {
        return addPredicate(Predicates.greaterEqual(qb.getAttribute(), value));
    }

    public PredicateBuilder lessThan(Comparable value) {
        return addPredicate(Predicates.lessThan(qb.getAttribute(), value));
    }

    public PredicateBuilder lessEqual(Comparable value) {
        return addPredicate(Predicates.lessEqual(qb.getAttribute(), value));
    }

    public PredicateBuilder between(Comparable from, Comparable to) {
        return addPredicate(Predicates.between(qb.getAttribute(), from, to));
    }

    public PredicateBuilder in(Comparable... values) {
        return addPredicate(Predicates.in(qb.getAttribute(), values));
    }

    private PredicateBuilder addPredicate(Predicate predicate) {
        qb.lsPredicates.add(predicate);
        return qb;
    }
}
