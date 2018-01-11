/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;

/**
 * @param <V> type of {@link AbstractRecord}
 */
abstract class AbstractRecordWithStats<V> extends AbstractRecord<V> {

    protected long lastStoredTime;
    protected long expirationTime;

    AbstractRecordWithStats() {
    }

    @Override
    public final void onStore() {
        lastStoredTime = Clock.currentTimeMillis();
    }

    @Override
    public long getCost() {
        final int numberOfLongFields = 2;
        return super.getCost() + numberOfLongFields * LONG_SIZE_IN_BYTES;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    @Override
    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    @Override
    public long getLastStoredTime() {
        return lastStoredTime;
    }

    @Override
    public void setLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = lastStoredTime;
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
        result = 31 * result + (int) (lastStoredTime ^ (lastStoredTime >>> 32));
        result = 31 * result + (int) (expirationTime ^ (expirationTime >>> 32));
        return result;
    }
}
