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

package com.hazelcast.query.impl;

import com.hazelcast.query.AttributePredicate;
import com.hazelcast.query.ConnectorPredicate;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.Predicate;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Provides the context of Query.
 */
public class QueryContext {
    private static final Comparator<Map.Entry<Integer, Index>> INDEX_COMPARATOR = new Comparator<Map.Entry<Integer, Index>>() {
        @Override
        public int compare(Map.Entry<Integer, Index> o1, Map.Entry<Integer, Index> o2) {
            return o2.getKey() - o1.getKey();
        }
    };

    private final IndexService indexService;
    private QueryPlan queryPlan;
    private final IndexAwarePredicate predicate;

    public QueryContext(IndexService indexService, IndexAwarePredicate predicate) {
        this.indexService = indexService;
        this.predicate = predicate;
    }

    private AttributePredicate findPredicate(ConnectorPredicate predicate, String attribute) {
        for (Predicate predicate1 : predicate.getPredicates()) {
            if (predicate1 instanceof AttributePredicate) {
                String attr = ((AttributePredicate) predicate1).getAttribute();
                if (attr.equals(attribute)) {
                    return (AttributePredicate) predicate1;
                }
            }
        }
        return null;
    }

    public QueryPlan getQueryPlan(boolean useIndexForSubsetPredicates) {
        if (queryPlan == null) {
            if (predicate instanceof ConnectorPredicate) {
                planQueryForConnectorPredicate(getIdealIndexes((ConnectorPredicate) predicate), useIndexForSubsetPredicates);
            } else {
                planQueryForSinglePredicate(useIndexForSubsetPredicates);
            }
        }
        return queryPlan;
    }

    private Queue<Map.Entry<Integer, Index>> getIdealIndexes(ConnectorPredicate predicate) {
        // we sort indexes for our query in order to optimize the count of indexes
        // that will be used.
        Index[] indexes = indexService.getIndexes();

        PriorityQueue<Map.Entry<Integer, Index>> q;
        q = new PriorityQueue<Map.Entry<Integer, Index>>(indexes.length, INDEX_COMPARATOR);
        for (Index index : indexes) {

            Predicate indexPredicate = index.getPredicate();
            if (indexPredicate != null) {
                ConnectorPredicate subtracted = predicate.subtract(indexPredicate);
                int rank;
                if (subtracted != null) {
                    rank = predicate.getPredicateCount() - subtracted.getPredicateCount();
                    if (findPredicate(predicate, index.getAttributeName()) != null) {
                        rank++;
                    }
                } else if (predicate.in(indexPredicate)) {
                    rank = 0;
                } else {
                    continue;
                }
                q.add(new AbstractMap.SimpleEntry<Integer, Index>(rank, index));
            } else {

                if (findPredicate(predicate, index.getAttributeName()) != null) {
                    q.add(new AbstractMap.SimpleEntry<Integer, Index>(1, index));
                }

            }

        }
        return q;
    }

    private void planQueryForSinglePredicate(boolean useIndexForSubsetPredicates) {
        List<IndexPredicate> plan = new ArrayList<IndexPredicate>(1);
        Index[] indexes = indexService.getIndexes();

        for (Index index : indexes) {
            Predicate p = index.getPredicate();
            if (p != null) {
                if ((p instanceof ConnectorPredicate && ((ConnectorPredicate) p).isSubset(predicate))
                        || p.equals(predicate)) {
                    plan.add(new IndexPredicate(predicate, index));
                    queryPlan = new QueryPlan(plan);
                    return;
                }
            }
        }

        if (useIndexForSubsetPredicates) {
            for (Index index : indexes) {
                Predicate p = index.getPredicate();
                if (p != null) {
                    if (predicate.in(p)) {
                        plan.add(new IndexPredicate(predicate, index));
                        HashMap<Predicate, Predicate> notIndexedPredicates = new HashMap<Predicate, Predicate>(1);
                        notIndexedPredicates.put(predicate, p);
                        queryPlan = new QueryPlan(plan, notIndexedPredicates);
                        return;
                    }
                }
            }
        }


    }

    private void planQueryForConnectorPredicate(Queue<Map.Entry<Integer, Index>> q, boolean useIndexForSubsetPredicates) {
        if (indexService == null && !(predicate instanceof ConnectorPredicate)) {
            return;
        }

        HashMap<Predicate, Predicate> mappedNotIndexedPredicatesList = new HashMap<Predicate, Predicate>();
        ConnectorPredicate lastPredicate = ((ConnectorPredicate) predicate).copy();
        LinkedList<IndexPredicate> plan = new LinkedList<IndexPredicate>();

        Map.Entry<Integer, Index> entry = q.poll();
        Index index;
        while (entry != null) {
            index = entry.getValue();
            // since the indexes are sorted, if predicate of the index doesn't have any common predicate
            // we can pass the other indexes
            AttributePredicate inlinePredicate = findPredicate(lastPredicate, index.getAttributeName());

            ConnectorPredicate subtractIndex = null;
            Predicate indexPredicate = index.getPredicate();
            if (indexPredicate != null) {
                subtractIndex = lastPredicate.subtract(indexPredicate);
                // if subtract is null, then lastPredicate is in indexPredicate. (only happens in AndPredicate)
                if (subtractIndex != null) {
                    lastPredicate = subtractIndex;
                } else if (entry.getKey() == 0 && useIndexForSubsetPredicates) {
                    Predicate predicates = extractInPredicate(lastPredicate, indexPredicate);
                    mappedNotIndexedPredicatesList.put(indexPredicate, predicates);
                }

            }

            if (inlinePredicate != null && inlinePredicate instanceof IndexAwarePredicate) {
                IndexAwarePredicate indexAware = (IndexAwarePredicate) inlinePredicate;
                plan.add(new IndexPredicate(indexAware, index));
                ConnectorPredicate subtractedInline = lastPredicate.subtract(inlinePredicate);
                if (subtractedInline != null) {
                    lastPredicate = subtractedInline;
                }
            } else if (subtractIndex != null) {
                plan.add(new IndexPredicate(null, index));
            }
            entry = q.poll();
        }


        queryPlan = new QueryPlan(plan, mappedNotIndexedPredicatesList, Arrays.asList(lastPredicate.getPredicates()));
    }

    public Index getIdealIndex(Predicate predicate) {
        if (predicate instanceof ConnectorPredicate) {
            return null;
        }
        if (queryPlan == null) {
            getQueryPlan(true);
        }
        List<IndexPredicate> plan = queryPlan.getPlan();
        for (IndexPredicate indexPredicate : plan) {
            if (indexPredicate.getPredicate().equals(predicate)) {
                return indexPredicate.getIndex();
            }

        }
        return null;
    }

    private Predicate extractInPredicate(ConnectorPredicate listPredicate, Predicate indexPredicate) {
        Predicate[] predicates = listPredicate.getPredicates();
        Predicate predicate;
        for (int i = 0; i < predicates.length; i++) {
            predicate = predicates[i];
            if (predicate instanceof ConnectorPredicate) {
                return extractInPredicate(listPredicate, indexPredicate);
            } else {
                if (predicate.in(indexPredicate)) {
                    listPredicate.removeChild(i);
                    return predicate;
                }
            }
        }
        return null;
    }

    /**
     * This class contains (predicate, index) pairs and predicates that didn't match any of the indexes.
     */
    public static final class QueryPlan {
        private final List<IndexPredicate> plan;
        private final Map<Predicate, Predicate> mappedNotIndexedPredicates;
        private final List<Predicate> notIndexedPredicates;

        public QueryPlan(List<IndexPredicate> queryPlan,
                         Map<Predicate, Predicate> mappedNotIndexedPredicates,
                         List<Predicate> notIndexedPredicates) {
            this.notIndexedPredicates = Collections.unmodifiableList(notIndexedPredicates);
            this.plan = Collections.unmodifiableList(queryPlan);
            this.mappedNotIndexedPredicates = Collections.unmodifiableMap(mappedNotIndexedPredicates);
        }

        public QueryPlan(List<IndexPredicate> queryPlan) {
            this.notIndexedPredicates = null;
            this.mappedNotIndexedPredicates = null;
            this.plan = Collections.unmodifiableList(queryPlan);
        }

        public QueryPlan(List<IndexPredicate> queryPlan, Map<Predicate, Predicate> mappedNotIndexedPredicates) {
            this.notIndexedPredicates = null;
            this.mappedNotIndexedPredicates = Collections.unmodifiableMap(mappedNotIndexedPredicates);
            this.plan = Collections.unmodifiableList(queryPlan);
        }

        public List<IndexPredicate> getPlan() {
            return plan;
        }

        public List<Predicate> getNotIndexedPredicates() {
            return notIndexedPredicates;
        }

        public Predicate getMappedNotIndexedPredicate(Predicate predicate) {
            return mappedNotIndexedPredicates.get(predicate);
        }

    }

    /**
     * This class contains (predicate, index) pair.
     * It will be used by predicates when querying index service.
     */
    public static final class IndexPredicate {
        private final IndexAwarePredicate predicate;
        private final Index index;

        public IndexPredicate(IndexAwarePredicate predicate, Index index) {
            this.predicate = predicate;
            this.index = index;
        }

        public IndexAwarePredicate getPredicate() {
            return predicate;
        }

        public Index getIndex() {
            return index;
        }
    }
}
