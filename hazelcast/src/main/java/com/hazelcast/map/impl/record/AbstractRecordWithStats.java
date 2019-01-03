/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.util.Clock;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;

/**
 * @param <V> type of {@link AbstractRecord}
 */
abstract class AbstractRecordWithStats<V>
        extends AbstractRecord<V> {

    private int lastStoredTime = NOT_AVAILABLE;
    private int expirationTime = NOT_AVAILABLE;

    AbstractRecordWithStats() {
    }

    @Override
    public final void onStore() {
        lastStoredTime = stripBaseTime(Clock.currentTimeMillis());
    }

    @Override
    public long getCost() {
        final int numberOfIntFields = 2;
        return super.getCost() + numberOfIntFields * INT_SIZE_IN_BYTES;
    }

    @Override
    public long getExpirationTime() {
        if (expirationTime == NOT_AVAILABLE) {
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
        if (expirationTime == NOT_AVAILABLE) {
            return 0L;
        }

        return recomputeWithBaseTime(lastStoredTime);
    }

    @Override
    public void setLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = stripBaseTime(lastStoredTime);
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
}
