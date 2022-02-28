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

package com.hazelcast.sql.impl.exec.scan.index;

import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import java.util.Iterator;

/**
 * Index filter which is transferred over a wire.
 */
@SuppressWarnings("rawtypes")
public interface IndexFilter {
    /**
     * Get index entries iterator
     *
     * @param index index
     * @param descending whether the entries should come in the descending order.
     *                   {@code true} means a descending order,
     *                   {@code false} means an ascending order.
     * @return iterator
     */
    Iterator<QueryableEntry> getEntries(InternalIndex index, boolean descending, ExpressionEvalContext evalContext);

    /**
     * Gets the value to be queried. Used by upper filter to construct the final lookup request.
     *
     * @return the value to be queried
     */
    Comparable getComparable(ExpressionEvalContext evalContext);
}
