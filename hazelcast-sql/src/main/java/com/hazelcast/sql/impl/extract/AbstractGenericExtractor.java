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

package com.hazelcast.sql.impl.extract;

import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * Base class for generic extractors.
 */
public abstract class AbstractGenericExtractor implements QueryExtractor {

    protected final boolean key;
    protected final QueryDataType type;
    protected final GenericTargetAccessor targetAccessor;

    protected AbstractGenericExtractor(boolean key, GenericTargetAccessor targetAccessor, QueryDataType type) {
        this.key = key;
        this.targetAccessor = targetAccessor;
        this.type = type;
    }
}
