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
import com.hazelcast.query.Predicates;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Provides the context of Query.
 */
public class QueryContext {
    private final IndexService indexService;
    private final List<Map.Entry<IndexAwarePredicate, Index>> queryPlan;
    private Predicate[] notIndexedPredicates;
    private final static Comparator<Map.Entry<Integer, Index>> indexComparator = new Comparator<Map.Entry<Integer, Index>>() {
        @Override
        public int compare(Map.Entry<Integer, Index> o1, Map.Entry<Integer, Index> o2) {
            return o2.getKey() - o1.getKey();
        }
    };

    public QueryContext(IndexService indexService, Predicate predicate) {
        this.indexService = indexService;
        this.queryPlan = new LinkedList<Map.Entry<IndexAwarePredicate, Index>>();
        if (predicate instanceof ConnectorPredicate) {
            planQuery((ConnectorPredicate) predicate);
        }
    }

    public Index getIndex(String attributeName) {
        for (Map.Entry<IndexAwarePredicate, Index> entry : queryPlan) {
            IndexAwarePredicate predicate = entry.getKey();
            if (predicate instanceof AttributePredicate) {
                if (((AttributePredicate) predicate).getAttribute().equals(attributeName)) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }

    public Index getIndex(Predicate predicate) {
        for (Index index : indexService.getIndexes()) {
            Predicate p = index.getPredicate();
            if (p != null) {
                if ((p instanceof ConnectorPredicate && ((ConnectorPredicate) p).isSubset(predicate))
                        || p.equals(predicate)) {
                    return index;
                }
            }
        }
        return null;
    }

    public Predicate[] getNotIndexedPredicates() {
        return notIndexedPredicates;
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

    public List<Map.Entry<IndexAwarePredicate, Index>> getQueryPlan() {
        return queryPlan;
    }

    private void planQuery(final ConnectorPredicate predicate) {
        if (indexService == null) {
            return;
        }

        Index[] indexes = indexService.getIndexes();

        // we sort indexes for our query in order to optimize the count of indexes
        // that will be used.


        PriorityQueue<Map.Entry<Integer, Index>> q = new PriorityQueue<Map.Entry<Integer, Index>>(11, indexComparator);
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
                }else
                if(predicate.in(indexPredicate)) {
                    rank = 0;
                }else {
                    continue;
                }
                q.add(new AbstractMap.SimpleEntry<Integer, Index>(rank, index));
            } else {

                if(findPredicate(predicate, index.getAttributeName())!=null) {
                    q.add(new AbstractMap.SimpleEntry<Integer, Index>(1, index));
                }

            }

        }
        LinkedList<Predicate> notIndexedPredicatesList = new LinkedList();

        ConnectorPredicate lastPredicate = predicate.copy();

        Map.Entry<Integer, Index> entry = q.poll();
        Index index;
        while (entry != null) {
            index = entry.getValue();
            // since the indexes are sorted, if predicate of the index doesn't have any common predicate
            // we can pass the other indexes
            AttributePredicate inlinePredicate = findPredicate(lastPredicate, index.getAttributeName());

            ConnectorPredicate subtract = null;
            Predicate indexPredicate = index.getPredicate();
            if (indexPredicate != null) {
                subtract = lastPredicate.subtract(indexPredicate);
                // if subtract is null, then lastPredicate is in indexPredicate. (only happens in AndPredicate)
                if(subtract!=null) {
                    lastPredicate = subtract;
                }else
                if(entry.getKey()==0 && predicate instanceof Predicates.AndPredicate) {
                    notIndexedPredicatesList.addAll(extractInPredicate(lastPredicate, indexPredicate));
                }

            }

            if (inlinePredicate!=null && inlinePredicate instanceof IndexAwarePredicate) {
                queryPlan.add(new AbstractMap.SimpleEntry<IndexAwarePredicate, Index>((IndexAwarePredicate) inlinePredicate, index));
                lastPredicate = lastPredicate.subtract(inlinePredicate);
            }else
            if(subtract!=null){
                queryPlan.add(new AbstractMap.SimpleEntry<IndexAwarePredicate, Index>(null, index));
            }
            entry = q.poll();
        }
        int size = notIndexedPredicatesList.size();

        notIndexedPredicates = new Predicate[size+lastPredicate.getPredicateCount()];

        notIndexedPredicatesList.toArray(notIndexedPredicates);
        for(int i= size; i<notIndexedPredicates.length; i++) {
            notIndexedPredicates[i] = lastPredicate.getPredicates()[i-size];
        }
    }

    private List<Predicate> extractInPredicate(ConnectorPredicate listPredicate, Predicate indexPredicate) {
        LinkedList<Predicate> extractedPredicates = new LinkedList<Predicate>();
        Predicate[] predicates = listPredicate.getPredicates();
        Predicate predicate;
        for (int i = 0; i < predicates.length; i++) {
            predicate = predicates[i];
            if(predicate instanceof ConnectorPredicate) {
                extractedPredicates.addAll(extractInPredicate(listPredicate, indexPredicate));
            }else {
                if(predicate.in(indexPredicate)) {
                    extractedPredicates.add(predicate);
                    listPredicate.removeChild(i);
                }
            }
        }
        return extractedPredicates;
    }
}
