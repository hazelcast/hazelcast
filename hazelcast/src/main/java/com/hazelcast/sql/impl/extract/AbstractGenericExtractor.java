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
