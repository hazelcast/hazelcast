/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.query;

public class EntryObject {
    PredicateBuilder qb;

    public EntryObject(PredicateBuilder qb) {
        this.qb = qb;
    }

    public EntryObject get(String property) {
        qb.exp = Predicates.get(property);
        return this;
    }

    public PredicateBuilder is(String property) {
        return addPredicate(Predicates.equal(Predicates.get(property), true));
    }

    public EntryObject key() {
        Expression expression = new EntryKeyObject();
        qb.exp = expression;
        return this;
    }

    public PredicateBuilder equal(Object value) {
        return addPredicate(Predicates.equal(qb.exp, value));
    }

    public PredicateBuilder greaterThan(Comparable value) {
        return addPredicate(Predicates.greaterThan(qb.exp, value));
    }

    public PredicateBuilder greaterEqual(Comparable value) {
        return addPredicate(Predicates.greaterEqual(qb.exp, value));
    }

    public PredicateBuilder lessThan(Comparable value) {
        return addPredicate(Predicates.lessThan(qb.exp, value));
    }

    public PredicateBuilder lessEqual(Comparable value) {
        return addPredicate(Predicates.lessEqual(qb.exp, value));
    }

    public PredicateBuilder between(Comparable from, Comparable to) {
        return addPredicate(Predicates.between(qb.exp, from, to));
    }

    public PredicateBuilder in(Comparable... values) {
        return addPredicate(Predicates.in(qb.exp, values));
    }

    private PredicateBuilder addPredicate(Predicate predicate) {
        qb.lsPredicates.add(predicate);
        return qb;
    }
}
