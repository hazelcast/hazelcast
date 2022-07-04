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

package com.hazelcast.sql.impl.extract;

import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * An interface that provides an indirection between {@link AbstractGenericExtractor} implementations and the parent
 * {@link QueryTarget}. It allows us to have different target implementations that produce same generic extractors.
 */
public interface GenericTargetAccessor {
    /**
     * Gets the target in the form suitable for field access.
     * <p>
     * For normal objects, the target is deserialized. For Portable and Compact objects, it is returned as {@code Data}.
     *
     * @return target in the form suitable for field access
     */
    Object getTargetForFieldAccess();

    /**
     * Gets the target for the direct access.
     *
     * @param type the expected target type
     * @return target for the direct access
     */
    Object getTargetForDirectAccess(QueryDataType type);
}
