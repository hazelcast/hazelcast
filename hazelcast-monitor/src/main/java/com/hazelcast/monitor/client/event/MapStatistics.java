/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
 *
 */
package com.hazelcast.monitor.client.event;

import java.io.Serializable;
import java.util.Collection;
import java.util.Date;

public class MapStatistics implements ChangeEvent, Serializable {
    private int clusterId;
    private Date date;

    Collection<LocalMapStatistics> listOfLocalStats;

    private int size;

    public MapStatistics() {

    }

    public MapStatistics(int clusterId) {
        this.clusterId = clusterId;
        this.date = new Date();
    }

    public ChangeEventType getChangeEventType() {
        return ChangeEventType.MAP_STATISTICS;
    }

    public int getClusterId() {
        return clusterId;
    }

    public Date getCreatedDate() {
        return date;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public Collection<LocalMapStatistics> getListOfLocalStats() {
        return listOfLocalStats;
    }

    public void setListOfLocalStats(Collection<LocalMapStatistics> listOfLocalStats) {
        this.listOfLocalStats = listOfLocalStats;
    }

    public static class LocalMapStatistics implements Serializable{

        public int ownedEntryCount;
        public int backupEntryCount;
        public int markedAsRemovedEntryCount;
        public int ownedEntryMemoryCost;
        public int backupEntryMemoryCost;
        public int markedAsRemovedMemoryCost;
        public long creationTime;
        public long lastAccessTime;
        public long lastUpdateTime;
        public long lastEvictionTime;
        public int hits;
        public int lockedEntryCount;
        public int lockWaitCount;
        public String memberName;
        public long periodStart;
        public long periodEnd;
        public long numberOfPutsInSec;
        public long numberOfGetsInSec;
        public long numberOfRemovesInSec;
        public long numberOfOthersInSec;

        public long totalOperationsInSec() {
            return numberOfGetsInSec + numberOfPutsInSec + numberOfRemovesInSec + numberOfOthersInSec;
        }
    }

}
