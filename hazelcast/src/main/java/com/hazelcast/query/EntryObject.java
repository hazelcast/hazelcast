package com.hazelcast.query;

import com.hazelcast.core.MapEntry;

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
        Expression expression = new Expression() {
            public Object getValue(Object obj) {
                MapEntry entry = (MapEntry) obj;
                return entry.getKey();
            }
        };

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
        qb.exp = null;
        return qb;
    }
}
