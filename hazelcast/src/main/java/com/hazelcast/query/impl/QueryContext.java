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

    private AttributePredicate findPredicate(Predicate predicate, String attribute) {
        if (predicate instanceof ConnectorPredicate) {
            for (Predicate predicate1 : ((ConnectorPredicate) predicate).getPredicates()) {
                if (predicate1 instanceof AttributePredicate) {
                    String attr = ((AttributePredicate) predicate1).getAttribute();
                    if (attr.equals(attribute)) {
                        return (AttributePredicate) predicate1;
                    }
                }
            }
        } else if (predicate instanceof AttributePredicate) {
            if (((AttributePredicate) predicate).getAttribute().equals(attribute)) {
                return (AttributePredicate) predicate;
            }
        }
        return null;
    }

    public QueryPlan getQueryPlan(boolean useIndexForSubsetPredicates) {
        if (queryPlan == null) {
            planQuery(getIdealIndexes(predicate), useIndexForSubsetPredicates);
        }
        return queryPlan;
    }

    private Integer getRankOfIndex(Index index) {
        Predicate indexPredicate = index.getPredicate();
        if (predicate instanceof ConnectorPredicate) {
            ConnectorPredicate p = (ConnectorPredicate) predicate;
            if (p.contains(indexPredicate)) {
                int count;
                Predicate subtracted = p.subtract(indexPredicate);
                if (subtracted instanceof ConnectorPredicate) {
                    count = ((ConnectorPredicate) subtracted).getPredicateCount();
                } else {
                    count = 1;
                }

                int rank = p.getPredicateCount() - count;
                if (findPredicate(predicate, index.getAttributeName()) != null) {
                    rank++;
                }
                return rank;
            } else if (predicate.isSubSet(indexPredicate)) {
                return 0;
            } else {
                return null;
            }
        } else {
            return 1;
        }
    }

    private Queue<Map.Entry<Integer, Index>> getIdealIndexes(Predicate predicate) {
        // we sort indexes for our query in order to optimize the count of indexes
        // that will be used.
        Index[] indexes = indexService.getIndexes();

        PriorityQueue<Map.Entry<Integer, Index>> q;
        q = new PriorityQueue<Map.Entry<Integer, Index>>(indexes.length, INDEX_COMPARATOR);
        for (Index index : indexes) {

            Predicate indexPredicate = index.getPredicate();
            if (indexPredicate != null) {
                Integer rank = getRankOfIndex(index);
                if (rank != null) {
                    q.add(new AbstractMap.SimpleEntry<Integer, Index>(rank, index));
                }
            } else {

                if (findPredicate(predicate, index.getAttributeName()) != null) {
                    q.add(new AbstractMap.SimpleEntry<Integer, Index>(1, index));
                }

            }

        }
        return q;
    }

    private void planQuery(Queue<Map.Entry<Integer, Index>> q, boolean useIndexForSubsetPredicates) {
        HashMap<Predicate, Predicate> mappedNotIndexedPredicates = new HashMap<Predicate, Predicate>();
        Predicate remainedPredicates = predicate;
        LinkedList<IndexPredicate> plan = new LinkedList<IndexPredicate>();

        for (Map.Entry<Integer, Index> entry : q) {
            Index index = entry.getValue();

            Predicate subtractIndex = null;
            Predicate indexPredicate = index.getPredicate();
            boolean indexContains = false;

            if (indexPredicate != null) {
                if (remainedPredicates instanceof ConnectorPredicate
                        && ((ConnectorPredicate) remainedPredicates).contains(indexPredicate)) {
                    subtractIndex = ((ConnectorPredicate) remainedPredicates).subtract(indexPredicate);
                    indexContains = true;
                } else {
                    indexContains = remainedPredicates.equals(indexPredicate);
                }

                if (indexContains && (subtractIndex instanceof ConnectorPredicate || subtractIndex == null)) {
                    remainedPredicates = subtractIndex;
                } else if (entry.getKey() == 0 && useIndexForSubsetPredicates) {
                    Predicate p = extractInPredicate(remainedPredicates, indexPredicate);
                    mappedNotIndexedPredicates.put(indexPredicate, p);
                    remainedPredicates = subtract(remainedPredicates, p);
                }
            }

            AttributePredicate inlinePredicate = findPredicate(remainedPredicates, index.getAttributeName());
            if (remainedPredicates != null && inlinePredicate != null && inlinePredicate instanceof IndexAwarePredicate) {
                remainedPredicates = subtract(remainedPredicates, inlinePredicate);
                plan.add(new IndexPredicate((IndexAwarePredicate) inlinePredicate, index));
            } else if (indexContains) {
                plan.add(new IndexPredicate(null, index));
            }
        }

        if (remainedPredicates == null) {
            queryPlan = new QueryPlan(plan, mappedNotIndexedPredicates);
        } else {
            queryPlan = new QueryPlan(plan, mappedNotIndexedPredicates, Arrays.asList(remainedPredicates));
        }
    }

    private Predicate subtract(Predicate predicate, Predicate compare) {
        if (predicate instanceof ConnectorPredicate) {
            return ((ConnectorPredicate) predicate).subtract(compare);
        } else if (predicate.equals(compare)) {
            return null;
        } else {
            throw new IllegalArgumentException();
        }
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
            IndexAwarePredicate p = indexPredicate.getPredicate();
            if (p == null && predicate.equals(indexPredicate.getIndex().getPredicate())) {
                return indexPredicate.getIndex();
            } else if (p.equals(predicate)) {
                return indexPredicate.getIndex();
            }

        }
        return null;
    }

    // the array just contains the reference of lastPredicate in its first index.
    private Predicate extractInPredicate(Predicate listPredicates, Predicate indexPredicate) {
        if (listPredicates instanceof ConnectorPredicate) {
            ConnectorPredicate p = (ConnectorPredicate) listPredicates;
            Predicate[] predicates = p.getPredicates();
            for (Predicate predicate : predicates) {
                if (predicate instanceof ConnectorPredicate) {
                    return extractInPredicate(predicate, indexPredicate);
                } else {
                    if (predicate.isSubSet(indexPredicate)) {
                        return predicate;
                    }
                }
            }
        } else {
            if (listPredicates.isSubSet(indexPredicate)) {
                return listPredicates;
            } else {
                return null;
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
            if (mappedNotIndexedPredicates == null) {
                return null;
            }
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
