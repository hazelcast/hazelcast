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

package com.hazelcast.map.writebehind;

/**
 * @param <K>
 */
abstract class AbstractDelayedEntry<K> {

    private final K key;

    private final long storeTime;

    // TODO really need this?
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

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final AbstractDelayedEntry delayedEntry = (AbstractDelayedEntry) obj;
        if (key == null) {
            return false;
        }
        return key.equals(delayedEntry.getKey());
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }
}
