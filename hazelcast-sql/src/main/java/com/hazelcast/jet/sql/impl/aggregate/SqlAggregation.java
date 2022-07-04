/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.nio.serialization.DataSerializable;

/**
 * A mutable object used for computing the SQL aggregation functions.
 */
public interface SqlAggregation extends DataSerializable {

    /**
     * Accumulate a value from a single row into this instance.
     */
    void accumulate(Object value);

    /**
     * Merge another aggregation into this aggregation.
     */
    void combine(SqlAggregation other);

    /**
     * Return the aggregation result.
     */
    Object collect();
}
