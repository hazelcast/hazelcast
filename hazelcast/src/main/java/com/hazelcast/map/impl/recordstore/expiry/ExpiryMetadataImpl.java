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

import static com.hazelcast.map.impl.record.Record.UNSET;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ExpiryMetadataImpl implements ExpiryMetadata {

    private int ttl;
    private int maxIdle;
    private int lastUpdateTime;
    private volatile int expirationTime;

    public ExpiryMetadataImpl() {
    }

    public ExpiryMetadataImpl(long ttl, long maxIdle,
                              long expirationTime, long lastUpdateTime) {
        setTtl(ttl);
        setMaxIdle(maxIdle);
        setExpirationTime(expirationTime);
        setLastUpdateTime(lastUpdateTime);
    }

    @Override
    public long getTtl() {
        return ttl == Integer.MAX_VALUE
                ? Long.MAX_VALUE : SECONDS.toMillis(ttl);
    }

    @Override
    public int getRawTtl() {
        return ttl;
    }

    @Override
    public ExpiryMetadata setTtl(long ttl) {
        this.ttl = toSeconds(ttl);
        return this;
    }

    @Override
    public ExpiryMetadata setRawTtl(int ttl) {
        this.ttl = ttl;
        return this;
    }

    @Override
    public long getMaxIdle() {
        return maxIdle == Integer.MAX_VALUE
                ? Long.MAX_VALUE : SECONDS.toMillis(maxIdle);
    }

    @Override
    public int getRawMaxIdle() {
        return maxIdle;
    }

    @Override
    public ExpiryMetadata setMaxIdle(long maxIdle) {
        this.maxIdle = toSeconds(maxIdle);
        return this;
    }

    private static int toSeconds(long millis) {
        long seconds = MILLISECONDS.toSeconds(millis);
        if (seconds == 0 && millis != 0) {
            seconds = 1;
        }
        return seconds > Integer.MAX_VALUE
                ? Integer.MAX_VALUE : (int) seconds;
    }

    @Override
    public ExpiryMetadata setRawMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
        return this;
    }

    @Override
    public long getExpirationTime() {
        if (expirationTime == UNSET) {
            return 0L;
        }

        if (expirationTime == Integer.MAX_VALUE) {
            return Long.MAX_VALUE;
        }

        return recomputeWithBaseTime(expirationTime);
    }

    @Override
    public int getRawExpirationTime() {
        return expirationTime;
    }

    @Override
    public ExpiryMetadata setExpirationTime(long expirationTime) {
        this.expirationTime = toStrippedSeconds(expirationTime);
        return this;
    }

    @Override
    public ExpiryMetadata setRawExpirationTime(int expirationTime) {
        this.expirationTime = expirationTime;
        return this;
    }

    @Override
    public long getLastUpdateTime() {
        if (lastUpdateTime == UNSET) {
            return 0L;
        }

        if (lastUpdateTime == Integer.MAX_VALUE) {
            return Long.MAX_VALUE;
        }

        return recomputeWithBaseTime(lastUpdateTime);
    }

    @Override
    public int getRawLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public ExpiryMetadata setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = toStrippedSeconds(lastUpdateTime);
        return this;
    }

    private int toStrippedSeconds(long millis) {
        if (millis == Long.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }

        return stripBaseTime(millis);
    }

    @Override
    public ExpiryMetadata setRawLastUpdateTime(int lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
        return this;
    }

    @Override
    public String toString() {
        return "ExpiryMetadataImpl{"
                + "ttl=" + getTtl()
                + ", maxIdle=" + getMaxIdle()
                + ", expirationTime=" + getExpirationTime()
                + ", lastUpdateTime=" + getLastUpdateTime()
                + '}';
    }
}
