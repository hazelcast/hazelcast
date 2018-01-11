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

package com.hazelcast.monitor.impl;

import com.hazelcast.monitor.LocalRecordStoreStats;

public class LocalRecordStoreStatsImpl implements LocalRecordStoreStats {
    private long hits;
    private long lastAccess;
    private long lastUpdate;

    @Override
    public long getHits() {
        return hits;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccess;
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdate;
    }

    @Override
    public void increaseHits() {
        this.hits++;
    }

    @Override
    public void increaseHits(long hits) {
        this.hits += hits;
    }

    @Override
    public void decreaseHits(long hits) {
        this.hits -= hits;
    }

    @Override
    public void setLastAccessTime(long time) {
        this.lastAccess = Math.max(this.lastAccess, time);
    }

    @Override
    public void setLastUpdateTime(long time) {
        this.lastUpdate = Math.max(this.lastUpdate, time);
    }

    public void reset() {
        this.hits = 0;
        this.lastAccess = 0;
        this.lastUpdate = 0;
    }
}
