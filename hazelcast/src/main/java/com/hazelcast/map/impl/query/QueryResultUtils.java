/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.PartitionPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.query.impl.predicates.PagingPredicateImpl;

import java.util.ArrayList;
import java.util.Set;

import static com.hazelcast.internal.util.SortingUtil.getSortedQueryResultSet;

public final class QueryResultUtils {

    private QueryResultUtils() {
    }

    public static Set transformToSet(
            SerializationService ss, QueryResult queryResult, Predicate predicate,
            IterationType iterationType, boolean unique, boolean binary) {
        Predicate unwrappedPredicate = unwrapPartitionPredicate(predicate);

        if (unwrappedPredicate instanceof PagingPredicate pagingPredicate) {
            // We need an instance of PagingPredicateImpl for later
            PagingPredicateImpl pagingPredicateImpl;
            if (unwrappedPredicate instanceof PagingPredicateImpl impl) {
                pagingPredicateImpl = impl;
            } else {
                Predicate simplePredicate = unwrappedPredicate::apply;
                // We provide Namespace wrapping to parent calls, we don't need to know it within the PagingPredicateImpl
                pagingPredicateImpl = new PagingPredicateImpl(simplePredicate, pagingPredicate.getComparator(),
                        pagingPredicate.getPageSize());
            }
            Set result = new QueryResultCollection(ss, IterationType.ENTRY, binary, unique, queryResult);
            return getSortedQueryResultSet(new ArrayList(result), pagingPredicateImpl, iterationType);
        } else {
            return new QueryResultCollection(ss, iterationType, binary, unique, queryResult);
        }
    }

    private static <K, V> Predicate<K, V> unwrapPartitionPredicate(Predicate<K, V> predicate) {
        return predicate instanceof PartitionPredicate ? ((PartitionPredicate<K, V>) predicate).getTarget() : predicate;
    }
}
