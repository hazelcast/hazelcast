/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression.datetime;

import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * Temporal value obtained through string parsing.
 */
public class TemporalValue {
    /** Type. */
    private final QueryDataType type;

    /** Value. */
    private final Object val;

    public TemporalValue(QueryDataType type, Object val) {
        this.type = type;
        this.val = val;
    }

    public QueryDataType getType() {
        return type;
    }

    public Object getValue() {
        return val;
    }
}
