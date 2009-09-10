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
            qb.lsPredicates.add(Predicates.equal(Predicates.get(property), true));
            qb.exp = null;
            return qb;
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
            qb.lsPredicates.add(Predicates.equal(qb.exp, value));
            qb.exp = null;
            return qb;
        }

        public PredicateBuilder greaterThan(Comparable value) {
            qb.lsPredicates.add(Predicates.greaterThan(qb.exp, value));
            qb.exp = null;
            return qb;
        }

        public PredicateBuilder lessThan(Comparable value) {
            qb.lsPredicates.add(Predicates.lessThan(qb.exp, value));
            qb.exp = null;
            return qb;
        }
    }
