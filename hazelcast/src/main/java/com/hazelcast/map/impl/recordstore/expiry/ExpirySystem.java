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

package com.hazelcast.map.impl.recordstore.expiry;

import com.hazelcast.internal.eviction.ExpiredKey;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.internal.serialization.Data;

/**
 * The expiry system interface that has all logic to remove expired entries.
 */
public interface ExpirySystem {

    @SuppressWarnings("AnonInnerLength")
    ExpirySystem NULL = new ExpirySystem() {

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public void evictExpiredEntries(int percentage, long now, boolean backup) {
           // no-op
        }

        @Override
        public ExpiryReason hasExpired(Data key, long now, boolean backup) {
            return ExpiryReason.NOT_EXPIRED;
        }

        @Override
        public InvalidationQueue<ExpiredKey> getExpiredKeys() {
            return new InvalidationQueue<>();
        }

        @Override
        public void extendExpiryTime(Data dataKey, long now) {
            // no-op
        }

        @Override
        public void accumulateOrSendExpiredKey(Data dataKey, long valueHashCode) {
            // no-op
        }

        @Override
        public ExpiryMetadata getExpiryMetadata(Data key) {
            return ExpiryMetadata.NULL;
        }

        @Override
        public void add(Data key, ExpiryMetadata expiryMetadata, long now) {
            // no-op
        }

        @Override
        public void add(Data key, long ttl, long maxIdle, long expiryTime, long lastUpdateTime, long now) {
            // no-op
        }

        @Override
        public void removeKeyFromExpirySystem(Data key) {
            // no-op
        }

        @Override
        public long calculateExpirationTime(long ttl, long maxIdle, long now, long lastUpdateTime) {
            return Long.MAX_VALUE;
        }

        @Override
        public void clear() {
            // no-op
        }

        @Override
        public void destroy() {
            // no-op
        }
    };

    boolean isEmpty();

    void evictExpiredEntries(int percentage, long now, boolean backup);

    ExpiryReason hasExpired(Data key, long now, boolean backup);

    InvalidationQueue<ExpiredKey> getExpiredKeys();

    void extendExpiryTime(Data dataKey, long now);

    void accumulateOrSendExpiredKey(Data dataKey, long valueHashCode);

    ExpiryMetadata getExpiryMetadata(Data key);

    void add(Data key, ExpiryMetadata expiryMetadata, long now);

    void add(Data key, long ttl, long maxIdle,
                          long expiryTime, long lastUpdateTime, long now);

    void removeKeyFromExpirySystem(Data key);

    long calculateExpirationTime(long ttl, long maxIdle,
                                              long now, long lastUpdateTime);

    void clear();

    void destroy();
}
