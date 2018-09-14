package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.VisitablePredicate;

import java.util.Map;

public class IndexSupressionPredicate implements VisitablePredicate {

    private final Predicate predicate;

    public IndexSupressionPredicate(Predicate p) {
        this.predicate = p;
    }

    @Override
    public Object visit(PredicateVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        return false;
    }
}
