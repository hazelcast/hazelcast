/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.AndResultSet;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * And Predicate
 */
public class AndPredicate implements IndexAwarePredicate, DataSerializable {

    protected Predicate[] predicates;

    public AndPredicate() {
    }

    public AndPredicate(Predicate... predicates) {
        this.predicates = predicates;
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Set<QueryableEntry> smallestIndexedResult = null;
        List<Set<QueryableEntry>> otherIndexedResults = new LinkedList<Set<QueryableEntry>>();
        List<Predicate> lsNoIndexPredicates = null;
        for (Predicate predicate : predicates) {
            boolean indexed = false;
            if (predicate instanceof IndexAwarePredicate) {
                IndexAwarePredicate iap = (IndexAwarePredicate) predicate;
                if (iap.isIndexed(queryContext)) {
                    indexed = true;
                    Set<QueryableEntry> s = iap.filter(queryContext);
                    if (smallestIndexedResult == null) {
                        smallestIndexedResult = s;
                    } else if (s.size() < smallestIndexedResult.size()) {
                        otherIndexedResults.add(smallestIndexedResult);
                        smallestIndexedResult = s;
                    } else {
                        otherIndexedResults.add(s);
                    }
                }
            }
            if (!indexed) {
                if (lsNoIndexPredicates == null) {
                    lsNoIndexPredicates = new LinkedList<Predicate>();
                }
                lsNoIndexPredicates.add(predicate);
            }
        }
        if (smallestIndexedResult == null) {
            return null;
        }
        return new AndResultSet(smallestIndexedResult, otherIndexedResults, lsNoIndexPredicates);
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        for (Predicate predicate : predicates) {
            if (predicate instanceof IndexAwarePredicate) {
                IndexAwarePredicate iap = (IndexAwarePredicate) predicate;
                if (iap.isIndexed(queryContext)) {
                    return true;
                }
            }
        }
        return false;
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
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("(");
        int size = predicates.length;
        for (int i = 0; i < size; i++) {
            if (i > 0) {
                sb.append(" AND ");
            }
            sb.append(predicates[i]);
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(predicates.length);
        for (Predicate predicate : predicates) {
            out.writeObject(predicate);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        predicates = new Predicate[size];
        for (int i = 0; i < size; i++) {
            predicates[i] = in.readObject();
        }
    }
}
