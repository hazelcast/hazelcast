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

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.map.impl.record.RecordReaderWriter.DATA_RECORD_WITH_STATS_READER_WRITER;

/**
 * @param <V> type of {@link AbstractRecord}
 */
abstract class AbstractRecordWithStats<V> extends AbstractRecord<V> {

    private int lastStoredTime = UNSET;
    private int expirationTime = UNSET;

    AbstractRecordWithStats() {
    }

    @Override
    public long getCost() {
        final int numberOfIntFields = 2;
        return super.getCost() + numberOfIntFields * INT_SIZE_IN_BYTES;
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
    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime == Long.MAX_VALUE
                ? Integer.MAX_VALUE
                : stripBaseTime(expirationTime);
    }

    @Override
    public long getLastStoredTime() {
        if (lastStoredTime == UNSET) {
            return 0L;
        }

        return recomputeWithBaseTime(lastStoredTime);
    }

    @Override
    public void setLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = stripBaseTime(lastStoredTime);
    }

    @Override
    public int getRawLastStoredTime() {
        return lastStoredTime;
    }

    @Override
    public void setRawLastStoredTime(int time) {
        this.lastStoredTime = time;
    }

    @Override
    public int getRawExpirationTime() {
        return expirationTime;
    }

    @Override
    public void setRawExpirationTime(int time) {
        this.expirationTime = time;
    }

    @Override
    public RecordReaderWriter getMatchingRecordReaderWriter() {
        return DATA_RECORD_WITH_STATS_READER_WRITER;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }

        AbstractRecordWithStats<?> that = (AbstractRecordWithStats<?>) o;
        if (lastStoredTime != that.lastStoredTime) {
            return false;
        }

        return expirationTime == that.expirationTime;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + lastStoredTime;
        result = 31 * result + expirationTime;
        return result;
    }

    @Override
    public String toString() {
        return "AbstractRecordWithStats{"
                + "lastStoredTime=" + lastStoredTime
                + ", expirationTime=" + expirationTime
                + "} " + super.toString();
    }
}
