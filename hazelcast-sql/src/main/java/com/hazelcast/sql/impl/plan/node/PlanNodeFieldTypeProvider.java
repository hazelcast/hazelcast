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

package com.hazelcast.sql.impl.plan.node;

import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * Interface to resolve field types of a single node.
 */
public interface PlanNodeFieldTypeProvider {

    PlanNodeFieldTypeProvider FAILING_FIELD_TYPE_PROVIDER = index -> {
        throw new IllegalStateException("The operation should not be called.");
    };

    /**
     * Gets the type of the operator's column at the given index (zero-based).
     *
     * @param index Index.
     * @return Type of the column.
     */
    QueryDataType getType(int index);
}
