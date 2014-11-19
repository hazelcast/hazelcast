/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.record;

import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.nio.serialization.Data;

import java.util.Map;

public interface CacheRecordMap<K extends Data, V extends CacheRecord> extends Map<K, V> {

    CacheKeyIteratorResult fetchNext(int nextTableIndex, int size);
    int evictExpiredRecords(int percentage);
    int evictRecords(int percentage, EvictionPolicy policy);

}
