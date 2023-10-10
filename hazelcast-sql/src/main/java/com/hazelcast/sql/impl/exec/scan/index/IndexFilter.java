/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.exec.scan.index;

import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

/**
 * Index filter which is transferred over a wire.
 */
@SuppressWarnings("rawtypes")
public interface IndexFilter {

    /**
     * Gets the value to be queried. Used by upper filter to construct the final lookup request.
     *
     * @return the value to be queried
     */
    Comparable getComparable(ExpressionEvalContext evalContext);

    /**
     * Indicates if all expressions used in IndexFilter are cooperative.
     **/
    boolean isCooperative();
}
