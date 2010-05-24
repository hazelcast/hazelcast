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

public class MapStatistics extends InstanceStatistics implements ChangeEvent, Serializable {

    Collection<LocalMapStatistics> listOfLocalStats;

    public MapStatistics() {
    }

    public MapStatistics(int clusterId) {
        super(clusterId);
    }

    public ChangeEventType getChangeEventType() {
        return ChangeEventType.MAP_STATISTICS;
    }

    public Collection<LocalMapStatistics> getListOfLocalStats() {
        return listOfLocalStats;
    }

    public void setListOfLocalStats(Collection<LocalMapStatistics> listOfLocalStats) {
        this.listOfLocalStats = listOfLocalStats;
        int tops = 0;
        for (MapStatistics.LocalMapStatistics l : listOfLocalStats) {
            tops += l.totalOperationsInSec();
            size += l.ownedEntryCount;
        }
        totalOPS = tops;
    }

    public static class LocalMapStatistics implements Serializable, LocalInstanceStatistics {

        public long ownedEntryCount;
        public long backupEntryCount;
        public long markedAsRemovedEntryCount;
        public long ownedEntryMemoryCost;
        public long backupEntryMemoryCost;
        public long markedAsRemovedMemoryCost;
        public long creationTime;
        public long lastAccessTime;
        public long lastUpdateTime;
        public long lastEvictionTime;
        public long hits;
        public long lockedEntryCount;
        public long lockWaitCount;
        public String memberName;
        public long periodStart;
        public long periodEnd;
        public long numberOfPutsInSec;
        public long numberOfGetsInSec;
        public long numberOfRemovesInSec;
        public long numberOfOthersInSec;
        public long numberOfEventsInSec;

        public long totalOperationsInSec() {
            return numberOfGetsInSec + numberOfPutsInSec + numberOfRemovesInSec + numberOfOthersInSec;
        }
    }
}
