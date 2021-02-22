/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.map.impl.record.Record.EPOCH_TIME;
import static com.hazelcast.map.impl.record.Record.UNSET;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public interface ExpiryMetadata {

    @SuppressWarnings("checkstyle:anoninnerlength")
    ExpiryMetadata NULL = new ExpiryMetadata() {
        @Override
        public long getTtl() {
            return Long.MAX_VALUE;
        }

        @Override
        public int getRawTtl() {
            return Integer.MAX_VALUE;
        }

        @Override
        public ExpiryMetadata setTtl(long ttl) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpiryMetadata setRawTtl(int ttl) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getMaxIdle() {
            return Long.MAX_VALUE;
        }

        @Override
        public int getRawMaxIdle() {
            return Integer.MAX_VALUE;
        }

        @Override
        public ExpiryMetadata setMaxIdle(long maxIdle) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpiryMetadata setRawMaxIdle(int maxIdle) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getExpirationTime() {
            return Long.MAX_VALUE;
        }

        @Override
        public int getRawExpirationTime() {
            return Integer.MAX_VALUE;
        }

        @Override
        public ExpiryMetadata setExpirationTime(long expirationTime) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpiryMetadata setRawExpirationTime(int expirationTime) {
            throw new UnsupportedOperationException();
        }
    };


    long getTtl();

    int getRawTtl();

    ExpiryMetadata setTtl(long ttl);

    ExpiryMetadata setRawTtl(int ttl);

    long getMaxIdle();

    int getRawMaxIdle();

    ExpiryMetadata setMaxIdle(long maxIdle);

    ExpiryMetadata setRawMaxIdle(int maxIdle);

    long getExpirationTime();

    int getRawExpirationTime();

    ExpiryMetadata setExpirationTime(long expirationTime);

    ExpiryMetadata setRawExpirationTime(int expirationTime);

    default int stripBaseTime(long value) {
        int diff = UNSET;
        if (value > 0) {
            diff = (int) MILLISECONDS.toSeconds(value - EPOCH_TIME);
        }

        return diff;
    }

    default long recomputeWithBaseTime(int value) {
        if (value == UNSET) {
            return 0L;
        }

        long exploded = SECONDS.toMillis(value);
        return exploded + EPOCH_TIME;
    }
}
