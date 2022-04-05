/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

        if (unwrappedPredicate instanceof PagingPredicate) {
            Set result = new QueryResultCollection(ss, IterationType.ENTRY, binary, unique, queryResult);
            return getSortedQueryResultSet(new ArrayList(result), (PagingPredicate) unwrappedPredicate, iterationType);
        } else {
            return new QueryResultCollection(ss, iterationType, binary, unique, queryResult);
        }
    }

    private static Predicate unwrapPartitionPredicate(Predicate predicate) {
        return predicate instanceof PartitionPredicate ? ((PartitionPredicate) predicate).getTarget() : predicate;
    }
}
