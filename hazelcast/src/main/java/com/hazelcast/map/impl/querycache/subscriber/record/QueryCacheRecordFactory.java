/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.querycache.subscriber.record;

import com.hazelcast.serialization.Data;

/**
 * Contract for various {@link QueryCacheRecord} factories.
 */
public interface QueryCacheRecordFactory {

    /**
     * Creates new {@link QueryCacheRecord}.
     *
     * @param value {@link Data} value
     * @return an instance of {@link QueryCacheRecord}
     */
    QueryCacheRecord createRecord(Data value);

    boolean isEquals(Object cacheRecordValue, Object value);
}
