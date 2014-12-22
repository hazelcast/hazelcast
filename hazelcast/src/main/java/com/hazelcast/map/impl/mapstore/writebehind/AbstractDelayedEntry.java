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

package com.hazelcast.map.impl.mapstore.writebehind;

/**
 * @param <K> the key type.
 */
abstract class AbstractDelayedEntry<K> {

    protected final K key;

    protected long storeTime;

    private final int partitionId;

    protected AbstractDelayedEntry(K key, long storeTime, int partitionId) {
        this.key = key;
        this.storeTime = storeTime;
        this.partitionId = partitionId;
    }

    public K getKey() {
        return key;
    }

    public long getStoreTime() {
        return storeTime;
    }

    public void setStoreTime(long storeTime) {
        this.storeTime = storeTime;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + (key == null ? 0 : key.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractDelayedEntry that = (AbstractDelayedEntry) o;

        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        }

        return true;
    }
}
