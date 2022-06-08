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

import static com.hazelcast.internal.util.TimeStripUtil.recomputeWithBaseTime;
import static com.hazelcast.internal.util.TimeStripUtil.stripBaseTime;
import static com.hazelcast.map.impl.ExpirationTimeSetter.toMillis;
import static com.hazelcast.map.impl.ExpirationTimeSetter.toSeconds;

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
        return toMillis(ttl);
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
        return toMillis(maxIdle);
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

    @Override
    public ExpiryMetadata setRawMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
        return this;
    }

    @Override
    public long getExpirationTime() {
        return recomputeWithBaseTime(expirationTime);
    }

    @Override
    public int getRawExpirationTime() {
        return expirationTime;
    }

    @Override
    public ExpiryMetadata setExpirationTime(long expirationTime) {
        this.expirationTime = stripBaseTime(expirationTime);
        return this;
    }

    @Override
    public ExpiryMetadata setRawExpirationTime(int expirationTime) {
        this.expirationTime = expirationTime;
        return this;
    }

    @Override
    public long getLastUpdateTime() {
        return recomputeWithBaseTime(lastUpdateTime);
    }

    @Override
    public int getRawLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public ExpiryMetadata setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = stripBaseTime(lastUpdateTime);
        return this;
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
