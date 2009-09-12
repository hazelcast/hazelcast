package com.hazelcast.query;

import com.hazelcast.core.MapEntry;
import static com.hazelcast.query.Predicates.*;

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
//        EntryObject e = new PredicateBuilder().getRoot();
//        Predicate p =
//                e.is("live")
//                        .and(e.get("salary").greaterThan(30).or(e.get("name").greaterThan(40)))
//                        .and(e.get("price").lessThan(50));
//        System.out.println(p);
        new PredicateBuilder().createPredicate("active=true and (age>20 or age <40)");
//        new PredicateBuilder().createPredicate("a and b AND(((a>=c AND b> d) OR (x <> y )) ) OR t>u");
    }

    public Predicate createPredicate(String sql) {
        Parser parser = new Parser();
        List<Object> tokens = new ArrayList<Object>(parser.toPrefix(sql));
        System.out.println(sql);
        root:
        while (tokens.size() > 1) {
//            System.out.println("token: " + tokens);
            for (int i = 0; i < tokens.size(); i++) {
                Object tokenObj = tokens.get(i);
                if (tokenObj instanceof String && Parser.isPrecedence((String) tokenObj)) {
                    String token = (String) tokenObj;
                    if ("=".equals(token) || "==".equals(token)) {
                        Object first = tokens.get(i - 2);
                        Object second = tokens.get(i - 1);
                        Predicate p = equal(get((String) first), second);
                        removeThreeAddOne(tokens, i, p);
                    } else if (">".equals(token)) {
                        Object first = tokens.get(i - 2);
                        Object second = tokens.get(i - 1);
                        Predicate p = greaterThan(get((String) first), (Comparable) second);
                        removeThreeAddOne(tokens, i, p);
                    } else if (">=".equals(token)) {
                        Object first = tokens.get(i - 2);
                        Object second = tokens.get(i - 1);
                        Predicate p = greaterEqual(get((String) first), (Comparable) second);
                        removeThreeAddOne(tokens, i, p);
                    } else if ("<=".equals(token)) {
                        Object first = tokens.get(i - 2);
                        Object second = tokens.get(i - 1);
                        Predicate p = lessEqual(get((String) first), (Comparable) second);
                        removeThreeAddOne(tokens, i, p);
                    } else if ("<".equals(token)) {
                        Object first = tokens.get(i - 2);
                        Object second = tokens.get(i - 1);
                        Predicate p = lessThan(get((String) first), (Comparable) second);
                        removeThreeAddOne(tokens, i, p);
                    } else if ("AND".equalsIgnoreCase(token)) {
                        Object first = tokens.get(i - 2);
                        Object second = tokens.get(i - 1);
                        Predicate p = Predicates.and((Predicate) first, (Predicate) second);
                        removeThreeAddOne(tokens, i, p);
                    } else if ("OR".equalsIgnoreCase(token)) {
                        Object first = tokens.get(i - 2);
                        Object second = tokens.get(i - 1);
                        Predicate p = Predicates.or((Predicate) first, (Predicate) second);
                        removeThreeAddOne(tokens, i, p);
                    }
                    continue root;
                }
            }
        }
        System.out.println(tokens.get(0));
        return (Predicate) tokens.get(0);
    }

    private void removeThreeAddOne(List<Object> tokens, int i, Object added) {
        tokens.remove(i - 2);
        tokens.remove(i - 2);
        tokens.remove(i - 2);
        tokens.add(i - 2, added);
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
