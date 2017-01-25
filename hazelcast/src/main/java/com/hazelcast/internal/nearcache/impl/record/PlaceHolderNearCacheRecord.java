/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nearcache.impl.record;

import com.hazelcast.internal.nearcache.NearCacheRecord;

import java.util.UUID;


public final class PlaceHolderNearCacheRecord implements NearCacheRecord {

    public static final NearCacheRecord UPDATE_STARTED_PLACEHOLDER = new PlaceHolderNearCacheRecord(UPDATE_STARTED);
    public static final NearCacheRecord REMOVE_REQUESTED_PLACEHOLDER = new PlaceHolderNearCacheRecord(REMOVE_REQUESTED);

    private final int recordState;

    public PlaceHolderNearCacheRecord(int recordState) {
        this.recordState = recordState;
    }

    @Override
    public int getRecordState() {
        return recordState;
    }

    @Override
    public boolean casRecordState(int expect, int update) {
        return false;
    }

    @Override
    public Object getValue() {
        return null;
    }

    @Override
    public boolean isExpiredAt(long now) {
        return false;
    }

    @Override
    public boolean isIdleAt(long maxIdleMilliSeconds, long now) {
        return false;
    }

    @Override
    public long getCreationTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLastAccessTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getAccessHit() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getExpirationTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setExpirationTime(long expirationTime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCreationTime(long time) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAccessTime(long time) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAccessHit(int hit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void incrementAccessHit() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resetAccessHit() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getInvalidationSequence() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setInvalidationSequence(long sequence) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setUuid(UUID uuid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasSameUuid(UUID uuid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "PlaceHolderNearCacheRecord{recordState=" + recordState + '}';
    }

}
