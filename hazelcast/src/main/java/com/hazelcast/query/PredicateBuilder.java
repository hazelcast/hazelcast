package com.hazelcast.query;

import com.hazelcast.core.MapEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PredicateBuilder implements Predicate, IndexAwarePredicate {
    Expression exp = null;
    List<Predicate> lsPredicates = new ArrayList<Predicate>();


    public boolean apply(MapEntry mapEntry) {
        return lsPredicates.get(0).apply(mapEntry);
    }

    public static void main(String[] args) {
        EntryObject e = new PredicateBuilder().getRoot();
        Predicate p =
                e.is("live")
                        .and(e.get("salary").greaterThan(30).or(e.get("name").greaterThan(40)))
                        .and(e.get("price").lessThan(50));
        System.out.println(p);

    }

    public boolean collectIndexAwarePredicates(List<IndexAwarePredicate> lsIndexPredicates, Map<Expression, Index<MapEntry>> mapIndexes) {
        boolean strong = true;
        Predicate predicate = lsPredicates.get(0);
        if (predicate instanceof IndexAwarePredicate) {
            IndexAwarePredicate p = (IndexAwarePredicate) predicate;
            if (!p.collectIndexAwarePredicates(lsIndexPredicates, mapIndexes)) {
                strong = false;
            }
        } else {
            strong = false;

        }
        return strong;
    }

    public Set<MapEntry> filter(Map<Expression, Index<MapEntry>> mapIndexes) {
        return null;
    }

    public void collectAppliedIndexes(Set<Index> setAppliedIndexes, Map<Expression, Index<MapEntry>> mapIndexes) {
        Predicate predicate = lsPredicates.get(0);
        if (predicate instanceof IndexAwarePredicate) {
            if (predicate instanceof IndexAwarePredicate) {
                IndexAwarePredicate p = (IndexAwarePredicate) predicate;
                p.collectAppliedIndexes(setAppliedIndexes, mapIndexes);
            }
        }
    }

    public EntryObject getRoot() {
        return new EntryObject(this);
    }

    public PredicateBuilder and(Predicate predicate) {
        int index = lsPredicates.size() - 2;
        Predicate first = lsPredicates.remove(index);
        Predicate second = lsPredicates.remove(index);
        lsPredicates.add(Predicates.and(first, second));
        return this;
    }

    public PredicateBuilder or(Predicate predicate) {
        int index = lsPredicates.size() - 2;
        Predicate first = lsPredicates.remove(index);
        Predicate second = lsPredicates.remove(index);
        lsPredicates.add(Predicates.or(first, second));
        return this;
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
