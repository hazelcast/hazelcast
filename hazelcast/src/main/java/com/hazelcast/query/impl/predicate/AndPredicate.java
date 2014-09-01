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

package com.hazelcast.query.impl.predicate;

import com.hazelcast.query.ConnectorPredicate;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.resultset.AndResultSet;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * And Predicate
 */
public class AndPredicate extends AbstractConnectorPredicate implements IndexAwarePredicate {

    public AndPredicate() {
    }

    public AndPredicate(Predicate predicate1, Predicate predicate2, Predicate... predicates) {
        super(predicate1, predicate2, predicates);
    }


    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Set<QueryableEntry> smallestIndexedResult = null;
        List<Set<QueryableEntry>> otherIndexedResults = new LinkedList<Set<QueryableEntry>>();

        QueryContext.QueryPlan queryPlan = queryContext.getQueryPlan(true);
        for (QueryContext.IndexPredicate entry : queryPlan.getPlan()) {
            IndexAwarePredicate key = entry.getPredicate();
            Set<QueryableEntry> s;
            if (key != null) {
                s = key.filter(queryContext);
            } else {
                s = entry.getIndex().getRecords();
            }
            if (smallestIndexedResult == null) {
                smallestIndexedResult = s;
            } else if (s.size() < smallestIndexedResult.size()) {
                otherIndexedResults.add(smallestIndexedResult);
                smallestIndexedResult = s;
            } else {
                otherIndexedResults.add(s);
            }

        }

        if (smallestIndexedResult == null) {
            return null;
        }
        return new AndResultSet(smallestIndexedResult, otherIndexedResults, queryPlan.getNotIndexedPredicates());
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        QueryContext.QueryPlan queryPlan = queryContext.getQueryPlan(true);
        return queryPlan.getPlan().size() > 0;
    }

    @Override
    public boolean equals(Object predicate) {
        if (predicate instanceof AndPredicate) {
            ConnectorPredicate p = (ConnectorPredicate) predicate;
            return this.contains(p) && p.contains(this);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(predicates);
    }

    @Override
    public String toString() {
        return toStringInternal("AND");
    }

    @Override
    public Predicate subtract(Predicate predicate) {
        if (!contains(predicate)) {
            throw new IllegalArgumentException("this.predicate must contain predicate");
        }

        List<Predicate> listPredicate = new LinkedList(Arrays.asList(predicates));
        if (!listPredicate.remove(predicate)) {
            if (predicate instanceof ConnectorPredicate) {
                for (Predicate p : ((ConnectorPredicate) predicate).getPredicates()) {
                    listPredicate.remove(p);
                }
            } else {
                listPredicate.remove(predicate);
            }
        }
        if (listPredicate.size() == 0) {
            return null;
        }
        if (listPredicate.size() == 1) {
            return listPredicate.get(0);
        }

        Predicate firstPredicate = listPredicate.remove(0);
        Predicate secondPredicate = listPredicate.remove(0);

        return new AndPredicate(firstPredicate, secondPredicate, listPredicate.toArray(new Predicate[listPredicate.size()]));
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        for (Predicate predicate : predicates) {
            if (!predicate.apply(mapEntry)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean contains(Predicate predicate) {
        if (predicate instanceof AndPredicate) {
            for (Predicate pInline : ((ConnectorPredicate) predicate).getPredicates()) {
                if (!this.contains(pInline)) {
                    return false;
                }
            }
            return true;

        } else {
            for (Predicate predicate1 : predicates) {
                if (predicate1.equals(predicate)) {
                    return true;
                } else if (predicate1 instanceof AndPredicate) {
                    if (((ConnectorPredicate) predicate1).contains(predicate)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

}
