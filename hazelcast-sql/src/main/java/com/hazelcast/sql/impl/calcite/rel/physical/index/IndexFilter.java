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

package com.hazelcast.sql.impl.calcite.rel.physical.index;

import org.apache.calcite.rex.RexNode;

/**
 * Represents a pair of conjunctive filters: one for the index lookup, the other one is remainder which should be applied
 * to the returned rows.
 */
public class IndexFilter {
    /** Index condition. */
    private final RexNode indexFilter;

    /** Remainder. */
    private final RexNode remainderFilter;

    public IndexFilter(RexNode indexFilter, RexNode remainderFilter) {
        this.indexFilter = indexFilter;
        this.remainderFilter = remainderFilter;
    }

    public RexNode getIndexFilter() {
        return indexFilter;
    }

    public RexNode getRemainderFilter() {
        return remainderFilter;
    }
}
