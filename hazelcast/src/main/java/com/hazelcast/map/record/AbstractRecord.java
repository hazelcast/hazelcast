/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.record;

import com.hazelcast.nio.serialization.Data;


@SuppressWarnings("VolatileLongOrDoubleField")
public abstract class AbstractRecord<V> implements Record<V> {

    protected RecordStatistics statistics;
    protected Data key;
    protected long version;
    /**
    *  this may be used for LRU or LFU eviction depending on configuration.
    * */
    protected long evictionCriteriaNumber;

    public AbstractRecord(Data key, boolean statisticsEnabled) {
        this.key = key;
        if (statisticsEnabled) {
            statistics = new RecordStatistics();
        }
        version = 0;
    }

    public AbstractRecord() {
    }

    public final Data getKey() {
        return key;
    }

    public final RecordStatistics getStatistics() {
        return statistics;
    }

    public final void setStatistics(RecordStatistics stats) {
        this.statistics = stats;
    }

    public final long getVersion() {
        return version;
    }

    public final void setVersion(long version) {
        this.version = version;
    }

    public final void onAccess() {
        if (statistics != null)
            statistics.access();
    }

    public final void onStore() {
        if (statistics != null)
            statistics.store();
    }

    public final void onUpdate() {
        if (statistics != null) {
            statistics.update();
        }
        version++;
    }

    @Override
    public long getEvictionCriteriaNumber() {
        return evictionCriteriaNumber;
    }

    @Override
    public void setEvictionCriteriaNumber(long evictionCriteriaNumber) {
       this.evictionCriteriaNumber = evictionCriteriaNumber;
    }

    @Override
    public long getCost() {
        int size = 0 ;
        // statistics
        size += 4 + (statistics == null ? 0 : statistics.size());
        // add key size.
        size += 4 + key.getHeapCost();
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractRecord that = (AbstractRecord) o;

        if (!key.equals(that.key)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public String toString() {
        return "Record{" + "key=" + key + '}';
    }

}
