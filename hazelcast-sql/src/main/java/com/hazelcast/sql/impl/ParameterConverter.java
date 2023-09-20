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

package com.hazelcast.sql.impl;

import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * A common interface to enforce a certain restriction on passed parameters.
 */
public interface ParameterConverter {
    /**
     * @return target parameter type
     */
    QueryDataType getTargetType();

    /**
     * Convert the original value of the parameter to the value of the target type
     *
     * @param value original value
     * @return converted value
     */
    Object convert(Object value);
}
