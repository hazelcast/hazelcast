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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * Target that is used to extract values from map entry's key or value.
 * <p>
 * Extractors are created once per query using the {@link #createExtractor(String, QueryDataType)} method. The target is then
 * updated for every map record using the {@link #setTarget(Object, Data)} method, while extractors remain the same.
 * <p>
 * The motivation for this design is to minimize the overhead on extractors creation and to avoid constant overhead associated
 * with data extraction, by maintaining the state. An example is {@code PortableGetter} that opens a reader on every get
 * operation. Instead, the reader might be opened in {@code setTarget} once and then reused for all fields.
 */
public interface QueryTarget {
    void setTarget(Object target, Data targetData);
    QueryExtractor createExtractor(String path, QueryDataType type);
}
