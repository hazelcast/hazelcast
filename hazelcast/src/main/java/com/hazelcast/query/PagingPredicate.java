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

package com.hazelcast.query;

import java.util.Comparator;
import java.util.Map;

/**
 * @deprecated As of release 3.4, replaced by {@link com.hazelcast.query.impl.predicate.PagingPredicate}
 */
@Deprecated
public class PagingPredicate extends com.hazelcast.query.impl.predicate.PagingPredicate {
    public PagingPredicate() {
    }

    public PagingPredicate(int pageSize) {
        super(pageSize);
    }

    public PagingPredicate(Predicate predicate, int pageSize) {
        super(predicate, pageSize);
    }

    public PagingPredicate(Comparator<Map.Entry> comparator, int pageSize) {
        super(comparator, pageSize);
    }

    public PagingPredicate(Predicate predicate, Comparator<Map.Entry> comparator, int pageSize) {
        super(predicate, comparator, pageSize);
    }
}
