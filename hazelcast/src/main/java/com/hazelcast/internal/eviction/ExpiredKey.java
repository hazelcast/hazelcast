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

package com.hazelcast.internal.eviction;

import com.hazelcast.internal.serialization.Data;

/**
 * Holds expired key and related metadata.
 */
public final class ExpiredKey {

    private final Data key;
    /**
     * Metadata is used to be sure that we delete correct entry on
     * backup replica. It can be creation-time or value-hash-code.
     *
     * @see com.hazelcast.map.impl.operation.EvictBatchBackupOperation#hasSameValueHashCode
     * @see com.hazelcast.cache.impl.operation.CacheExpireBatchBackupOperation#evictIfSame
     */
    private final long metadata;

    public ExpiredKey(Data key, long metadata) {
        this.key = key;
        this.metadata = metadata;
    }

    public Data getKey() {
        return key;
    }

    public long getMetadata() {
        return metadata;
    }
}
