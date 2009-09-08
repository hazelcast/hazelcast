package com.hazelcast.query;

import com.hazelcast.core.MapEntry;

import java.util.ArrayList;
import java.util.List;

public class QueryBuilder implements Predicate {
    Expression exp;
    List<Predicate> lsPredicates = new ArrayList<Predicate>();


    public boolean apply(MapEntry mapEntry) {
        return lsPredicates.get(0).apply(mapEntry);
    }

    public static void main(String[] args) {
        DomainObject e = new DomainObject(new QueryBuilder());
        Predicate p =
                e.method("isLive").equal(true)
                        .and(e.method("getSalary").greaterThan(30).or(e.method("getName").greaterThan(40)))
                        .and(e.method("getPrice").greaterThan(50));

        System.out.println(p);
    }

    public QueryBuilder and(Predicate predicate) {
        int index = lsPredicates.size() - 2;
        Predicate first = lsPredicates.remove(index);
        Predicate second = lsPredicates.remove(index);
        lsPredicates.add(Predicates.and(first, second));
        return this;
    }

    public QueryBuilder or(Predicate predicate) {
        int index = lsPredicates.size() - 2;
        Predicate first = lsPredicates.remove(index);
        Predicate second = lsPredicates.remove(index);
        lsPredicates.add(Predicates.or(first, second));
        return this;
    }

    public static class DomainObject {
        QueryBuilder qb;

        public DomainObject(QueryBuilder qb) {
            this.qb = qb;
        }

        public DomainObject method(String methodName) {
            qb.exp = Predicates.get(methodName);
            return this;
        }

        public DomainObject key() {
            Expression expression = new Expression() {
                public Object getValue(Object obj) {
                    MapEntry entry = (MapEntry) obj;
                    return entry.getKey();
                }
            };

            qb.exp = expression;
            return this;
        }

        public QueryBuilder equal(Object value) {
            qb.lsPredicates.add(Predicates.equal(qb.exp, value));
            qb.exp = null;
            return qb;
        }

        public QueryBuilder greaterThan(Comparable value) {
            qb.lsPredicates.add(Predicates.greaterThan(qb.exp, value));
            qb.exp = null;
            return qb;
        }

        public QueryBuilder lessThan(Object value) {
            return qb;
        }
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("QueryBuilder");
        sb.append("{\n");
        sb.append(lsPredicates.get(0));
        sb.append("\n}");
        return sb.toString();
    }
}
