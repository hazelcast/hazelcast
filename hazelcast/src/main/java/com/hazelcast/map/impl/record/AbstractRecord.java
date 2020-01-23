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

package com.hazelcast.map.impl.record;

import com.hazelcast.query.impl.Metadata;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Objects;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.map.impl.record.RecordReaderWriter.DATA_RECORD_READER_WRITER;

/**
 * @param <V> the type of the value of Record.
 */
@SuppressWarnings({"checkstyle:methodcount", "VolatileLongOrDoubleField"})
public abstract class AbstractRecord<V> implements Record<V> {

    private static final int NUMBER_OF_LONGS = 1;
    private static final int NUMBER_OF_INTS = 6;

    protected int ttl;
    protected int maxIdle;
    protected long version;

    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "Record can be accessed by only its own partition thread.")
    protected volatile int hits;
    private volatile int lastAccessTime = UNSET;
    private volatile int lastUpdateTime = UNSET;

    private int creationTime = UNSET;
    // TODO add cost of metadata to memory-cost calculations
    private transient Metadata metadata;

    AbstractRecord() {
    }

    @Override
    public RecordReaderWriter getMatchingRecordReaderWriter() {
        return DATA_RECORD_READER_WRITER;
    }

    @Override
    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public final long getVersion() {
        return version;
    }

    @Override
    public final void setVersion(long version) {
        this.version = version;
    }

    @Override
    public long getLastAccessTime() {
        return recomputeWithBaseTime(lastAccessTime);
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = stripBaseTime(lastAccessTime);
    }

    @Override
    public long getLastUpdateTime() {
        return recomputeWithBaseTime(lastUpdateTime);
    }

    @Override
    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = stripBaseTime(lastUpdateTime);
    }

    @Override
    public long getCreationTime() {
        return recomputeWithBaseTime(creationTime);
    }

    @Override
    public void setCreationTime(long creationTime) {
        this.creationTime = stripBaseTime(creationTime);
    }

    @Override
    public int getHits() {
        return hits;
    }

    @Override
    public void setHits(int hits) {
        this.hits = hits;
    }

    @Override
    public long getCost() {
        return (NUMBER_OF_LONGS * LONG_SIZE_IN_BYTES)
                + (NUMBER_OF_INTS * INT_SIZE_IN_BYTES);
    }

    @Override
    public Object getCachedValueUnsafe() {
        return Record.NOT_CACHED;
    }

    @Override
    public boolean casCachedValue(Object expectedValue, Object newValue) {
        return true;
    }

    @Override
    public final long getSequence() {
        return UNSET;
    }

    @Override
    public final void setSequence(long sequence) {
    }

    @Override
    public long getExpirationTime() {
        return UNSET;
    }

    @Override
    public void setExpirationTime(long expirationTime) {
    }

    @Override
    public long getLastStoredTime() {
        return UNSET;
    }

    @Override
    public void setLastStoredTime(long lastStoredTime) {
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractRecord<?> that = (AbstractRecord<?>) o;

        if (ttl != that.ttl) {
            return false;
        }
        if (maxIdle != that.maxIdle) {
            return false;
        }
        if (version != that.version) {
            return false;
        }
        if (hits != that.hits) {
            return false;
        }
        if (lastAccessTime != that.lastAccessTime) {
            return false;
        }
        if (lastUpdateTime != that.lastUpdateTime) {
            return false;
        }
        if (creationTime != that.creationTime) {
            return false;
        }
        return Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        int result = 31 * ttl + maxIdle;
        result = 31 * result + (int) (version ^ (version >>> 32));
        result = 31 * result + hits;
        result = 31 * result + lastAccessTime;
        result = 31 * result + lastUpdateTime;
        result = 31 * result + creationTime;
        result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
        return result;
    }

    @Override
    public int getRawTtl() {
        return ttl;
    }

    @Override
    public int getRawMaxIdle() {
        return maxIdle;
    }

    @Override
    public int getRawCreationTime() {
        return creationTime;
    }

    @Override
    public int getRawLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public int getRawLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public void setRawTtl(int ttl) {
        this.ttl = ttl;
    }

    @Override
    public void setRawMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    @Override
    public void setRawCreationTime(int creationTime) {
        this.creationTime = creationTime;
    }

    @Override
    public void setRawLastAccessTime(int lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    @Override
    public void setRawLastUpdateTime(int lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    @Override
    public int getRawLastStoredTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRawLastStoredTime(int time) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRawExpirationTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRawExpirationTime(int time) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "AbstractRecord{"
                + ", ttl=" + ttl
                + ", maxIdle=" + maxIdle
                + ", version=" + version
                + ", hits=" + hits
                + ", lastAccessTime=" + lastAccessTime
                + ", lastUpdateTime=" + lastUpdateTime
                + ", creationTime=" + creationTime
                + ", metadata=" + metadata
                + '}';
    }
}
