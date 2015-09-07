/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.nearcache.impl;

import com.hazelcast.eviction.impl.strategy.sampling.SampleableEvictableStore;
import com.hazelcast.cache.impl.nearcache.NearCacheRecord;

/**
 * Contract point for all record maps these supports entry sampling to be used for storage in near-cache.
 *
 * @param <K> type of the key
 * @param <V> type of the {@link com.hazelcast.cache.impl.nearcache.NearCacheRecord} to be stored
 *
 * @see com.hazelcast.cache.impl.nearcache.NearCacheRecord
 * @see com.hazelcast.cache.impl.nearcache.impl.NearCacheRecordMap
 * @see com.hazelcast.eviction.impl.strategy.sampling.SampleableEvictableStore
 */
public interface SampleableNearCacheRecordMap<K, V extends NearCacheRecord>
        extends NearCacheRecordMap<K, V>, SampleableEvictableStore<K, V> {

}
