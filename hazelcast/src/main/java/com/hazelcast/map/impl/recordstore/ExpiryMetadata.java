/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

public interface ExpiryMetadata {

    ExpiryMetadata NULL = new ExpiryMetadata() {
        @Override
        public long getTtl() {
            return Long.MAX_VALUE;
        }

        @Override
        public ExpiryMetadata setTtl(long ttl) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getMaxIdle() {
            return Long.MAX_VALUE;
        }

        @Override
        public ExpiryMetadata setMaxIdle(long maxIdle) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getExpirationTime() {
            return Long.MAX_VALUE;
        }

        @Override
        public ExpiryMetadata setExpirationTime(long expirationTime) {
            throw new UnsupportedOperationException();
        }
    };


    long getTtl();

    ExpiryMetadata setTtl(long ttl);

    long getMaxIdle();

    ExpiryMetadata setMaxIdle(long maxIdle);

    long getExpirationTime();

    ExpiryMetadata setExpirationTime(long expirationTime);
}
