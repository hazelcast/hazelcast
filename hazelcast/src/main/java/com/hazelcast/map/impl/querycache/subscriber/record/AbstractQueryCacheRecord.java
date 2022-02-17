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

package com.hazelcast.map.impl.querycache.subscriber.record;

import com.hazelcast.internal.util.Clock;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;


/**
 * Contains common functionality which is needed by a {@link QueryCacheRecord} instance.
 */
abstract class AbstractQueryCacheRecord implements QueryCacheRecord {

    private static final AtomicIntegerFieldUpdater<AbstractQueryCacheRecord> ACCESS_HIT =
            newUpdater(AbstractQueryCacheRecord.class, "hits");

    private final long creationTime;

    private volatile int hits;
    private volatile long accessTime = -1L;

    AbstractQueryCacheRecord() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public long getHits() {
        return hits;
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
    public void incrementAccessHit() {
        ACCESS_HIT.incrementAndGet(this);
    }

    @Override
    public void setAccessTime(long accessTime) {
        this.accessTime = accessTime;
    }
}
