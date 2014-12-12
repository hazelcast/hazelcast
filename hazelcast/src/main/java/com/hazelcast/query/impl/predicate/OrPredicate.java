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
import com.hazelcast.query.impl.resultset.OrResultSet;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Or Predicate
 */
public class OrPredicate extends AbstractConnectorPredicate implements IndexAwarePredicate {

    public OrPredicate(Predicate predicate1, Predicate predicate2, Predicate... predicates) {
        super(predicate1, predicate2, predicates);
    }

    public OrPredicate() {
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        List<Set<QueryableEntry>> indexedResults = new LinkedList<Set<QueryableEntry>>();

        QueryContext.QueryPlan queryPlan = queryContext.getQueryPlan(false);
        for (QueryContext.IndexPredicate entry : queryPlan.getPlan()) {
            IndexAwarePredicate key = entry.getPredicate();
            if (key != null) {
                indexedResults.add(key.filter(queryContext));
            } else {
                indexedResults.add(entry.getIndex().getRecords());
            }
        }

        return indexedResults.isEmpty() ? null : new OrResultSet(indexedResults);
    }

    @Deprecated
    public boolean equals(Object predicate) {
        if (predicate instanceof OrPredicate) {
            OrPredicate orPredicate = ((OrPredicate) predicate);
            outer:
            for (Predicate predicate1 : predicates) {
                for (Predicate predicate2 : orPredicate.getPredicates()) {
                    if (predicate1.equals(predicate2)) {
                        continue outer;
                    }
                }
                return false;
            }
            return orPredicate.getPredicateCount() == getPredicateCount();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(predicates);
    }

    @Override
    public String toString() {
        return toStringInternal("OR");
    }

    public boolean isIndexed(QueryContext queryContext) {
        QueryContext.QueryPlan queryPlan = queryContext.getQueryPlan(false);
        List<Predicate> notIndexedPredicates = queryPlan.getNotIndexedPredicates();
        return notIndexedPredicates == null || notIndexedPredicates.isEmpty();
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        for (Predicate predicate : predicates) {
            if (predicate.apply(mapEntry)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Predicate subtract(Predicate predicate) {
        if (!contains(predicate)) {
            throw new IllegalArgumentException("this.predicate must contain predicate");
        }

        List<Predicate> listPredicate = new LinkedList(Arrays.asList(predicates));
        if (predicate instanceof ConnectorPredicate) {
            for (Predicate p : ((ConnectorPredicate) predicate).getPredicates()) {
                listPredicate.remove(p);
            }
        } else {
            listPredicate.remove(predicate);
        }

        if (listPredicate.size() == 1) {
            return listPredicate.get(0);
        }

        if (listPredicate.size() == 0) {
            return null;
        }
        if (listPredicate.size() == 1) {
            return listPredicate.get(0);
        }

        Predicate firstPredicate = listPredicate.remove(0);
        Predicate secondPredicate = listPredicate.remove(0);
        return new OrPredicate(firstPredicate, secondPredicate, listPredicate.toArray(new Predicate[listPredicate.size()]));
    }

    @Override
    public boolean contains(Predicate predicate) {
        if (predicate instanceof OrPredicate) {
            for (Predicate pInline : ((ConnectorPredicate) predicate).getPredicates()) {
                if (!this.contains(pInline)) {
                    return false;
                }
            }

            return true;
        } else {
            for (Predicate p : predicates) {
                if (p instanceof OrPredicate) {
                    if (((ConnectorPredicate) p).contains(predicate)) {
                        return true;
                    }
                } else if (predicate.equals(p)) {
                    return true;
                }
            }
        }
        return false;
    }

}
