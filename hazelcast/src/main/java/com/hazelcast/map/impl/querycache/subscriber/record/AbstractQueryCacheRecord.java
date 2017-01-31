/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.util.Clock;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Contains common functionality which is needed by a {@link QueryCacheRecord} instance.
 */
abstract class AbstractQueryCacheRecord implements QueryCacheRecord {

    protected final long creationTime;
    protected volatile long accessTime = -1L;
    protected volatile int accessHit;

    public AbstractQueryCacheRecord() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public int getAccessHit() {
        return accessHit;
    }

    @Override
    public long getLastAccessTime() {
        return accessTime;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "CacheRecord can be accessed by only its own partition thread.")
    public void incrementAccessHit() {
        accessHit++;
    }

    @Override
    public void setAccessTime(long accessTime) {
        this.accessTime = accessTime;
    }
}
