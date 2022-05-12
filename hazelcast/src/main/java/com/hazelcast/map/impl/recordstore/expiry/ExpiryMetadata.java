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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public interface ExpiryMetadata {

    @SuppressWarnings("checkstyle:anoninnerlength")
    ExpiryMetadata NULL = new ExpiryMetadata() {
        @Override
        public boolean hasExpiry() {
            return false;
        }

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

        @Override
        public long getLastUpdateTime() {
            return 0;
        }

        @Override
        public int getRawLastUpdateTime() {
            return 0;
        }

        @Override
        public ExpiryMetadata setLastUpdateTime(long lastUpdateTime) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpiryMetadata setRawLastUpdateTime(int lastUpdateTime) {
            throw new UnsupportedOperationException();
        }
    };

    default boolean hasExpiry() {
        return true;
    }

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

    long getLastUpdateTime();

    int getRawLastUpdateTime();

    ExpiryMetadata setLastUpdateTime(long lastUpdateTime);

    ExpiryMetadata setRawLastUpdateTime(int lastUpdateTime);

    default void write(ObjectDataOutput out) throws IOException {
        out.writeInt(getRawTtl());
        out.writeInt(getRawMaxIdle());
        out.writeInt(getRawExpirationTime());
        out.writeInt(getRawLastUpdateTime());
    }

    default void read(ObjectDataInput in) throws IOException {
        setRawTtl(in.readInt());
        setRawMaxIdle(in.readInt());
        setRawExpirationTime(in.readInt());
        setRawLastUpdateTime(in.readInt());
    }
}
