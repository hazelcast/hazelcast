/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query;

import com.hazelcast.query.impl.predicates.PagingPredicateImpl;
import com.hazelcast.util.IterationType;

import java.util.Map;

/**
 * This class provides paging accessor of predicate.
 * <p>
 * This class is for internal use only and may be changed without any notice.
 */
public final class PagingPredicateAccessor {

    private PagingPredicateAccessor() {
    }

    public static void setAnchor(PagingPredicate predicate, int page, Map.Entry anchor) {
        ((PagingPredicateImpl) predicate).setAnchor(page, anchor);
    }

    public static Map.Entry<Integer, Map.Entry> getNearestAnchorEntry(PagingPredicate predicate) {
        if (predicate == null) {
            return null;
        }
        return ((PagingPredicateImpl) predicate).getNearestAnchorEntry();
    }

    public static IterationType getIterationType(PagingPredicate predicate) {
        return ((PagingPredicateImpl) predicate).getIterationType();
    }

    public static void setIterationType(PagingPredicate predicate, IterationType iterationType) {
        ((PagingPredicateImpl) predicate).setIterationType(iterationType);
    }

}
