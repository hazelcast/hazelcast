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
abstract class AbstractRecord<V> extends AbstractBaseRecord<V> {

    protected Data key;

    public AbstractRecord(Data key) {
        this.key = key;
    }

    public AbstractRecord() {
    }

    public final Data getKey() {
        return key;
    }

    public RecordStatistics getStatistics() {
        return null;
    }

    public void setStatistics(RecordStatistics stats) {
    }

    public void onAccess() {
        lastAccessTime = System.nanoTime();
    }

    public void onStore() {
    }

    public void onUpdate() {
        lastUpdateTime = System.nanoTime();
        version++;
    }

    @Override
    public Object getCachedValue() {
        return Record.NOT_CACHED;
    }

    @Override
    public void setCachedValue(Object cachedValue) {

    }

    @Override
    public long getCost() {
        int size = 0;
        final int objectReferenceInBytes = 4;
        // add size of version.
        size += (Long.SIZE / Byte.SIZE);
        // add size of evictionCriteriaNumber.
        size += (Long.SIZE / Byte.SIZE);
        // add key size.
        size += objectReferenceInBytes + key.getHeapCost();
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractRecord that = (AbstractRecord) o;
        return key.equals(that.key);
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
