/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.jmx;

import com.hazelcast.internal.jmx.suppliers.LocalMapStatsSupplier;
import com.hazelcast.internal.jmx.suppliers.StatsSupplier;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;

/**
 * Management bean for {@link IMap}
 */
@SuppressWarnings({"checkstyle:methodcount"})
@ManagedDescription("IMap")
public class MapMBean extends HazelcastMBean<IMap> {

    private final LocalStatsDelegate<LocalMapStats> localMapStatsDelegate;

    protected MapMBean(IMap managedObject, ManagementService service) {
        super(managedObject, service);
        this.objectName = service.createObjectName("IMap", managedObject.getName());
        StatsSupplier<LocalMapStats> localMapStatsSupplier = new LocalMapStatsSupplier(managedObject);
        this.localMapStatsDelegate = new LocalStatsDelegate<>(localMapStatsSupplier, updateIntervalSec);
    }

    @ManagedAnnotation("localOwnedEntryCount")
    @ManagedDescription("number of entries owned on this member")
    public long getLocalOwnedEntryCount() {
        return localMapStatsDelegate.getLocalStats().getOwnedEntryCount();
    }

    @ManagedAnnotation("localBackupEntryCount")
    @ManagedDescription("the number of backup entries hold on this member")
    public long getLocalBackupEntryCount() {
        return localMapStatsDelegate.getLocalStats().getBackupEntryCount();
    }

    @ManagedAnnotation("localBackupCount")
    @ManagedDescription("the number of backups per entry on this member")
    public int getLocalBackupCount() {
        return localMapStatsDelegate.getLocalStats().getBackupCount();
    }

    @ManagedAnnotation("localOwnedEntryMemoryCost")
    @ManagedDescription("memory cost (number of bytes) of owned entries on this member")
    public long getLocalOwnedEntryMemoryCost() {
        return localMapStatsDelegate.getLocalStats().getOwnedEntryMemoryCost();
    }

    @ManagedAnnotation("localBackupEntryMemoryCost")
    @ManagedDescription("memory cost (number of bytes) of backup entries on this member.")
    public long getLocalBackupEntryMemoryCost() {
        return localMapStatsDelegate.getLocalStats().getBackupEntryMemoryCost();
    }

    @ManagedAnnotation("localCreationTime")
    @ManagedDescription("the creation time of this map on this member.")
    public long getLocalCreationTime() {
        return localMapStatsDelegate.getLocalStats().getCreationTime();
    }

    @ManagedAnnotation("localLastAccessTime")
    @ManagedDescription("the last access (read) time of the locally owned entries.")
    public long getLocalLastAccessTime() {
        return localMapStatsDelegate.getLocalStats().getLastAccessTime();
    }

    @ManagedAnnotation("localLastUpdateTime")
    @ManagedDescription("the last update time of the locally owned entries.")
    public long getLocalLastUpdateTime() {
        return localMapStatsDelegate.getLocalStats().getLastUpdateTime();
    }

    @ManagedAnnotation("localHits")
    @ManagedDescription("the number of hits (reads) of the locally owned entries.")
    public long getLocalHits() {
        return localMapStatsDelegate.getLocalStats().getHits();
    }

    @ManagedAnnotation("localLockedEntryCount")
    @ManagedDescription("the number of currently locked locally owned keys.")
    public long getLocalLockedEntryCount() {
        return localMapStatsDelegate.getLocalStats().getLockedEntryCount();
    }

    @ManagedAnnotation("localDirtyEntryCount")
    @ManagedDescription("the number of entries that the member owns and are dirty on this member")
    public long getLocalDirtyEntryCount() {
        return localMapStatsDelegate.getLocalStats().getDirtyEntryCount();
    }

    @ManagedAnnotation("localPutOperationCount")
    @ManagedDescription("the number of put operations on this member")
    public long getLocalPutOperationCount() {
        return localMapStatsDelegate.getLocalStats().getPutOperationCount();
    }

    @ManagedAnnotation("localSetOperationCount")
    @ManagedDescription("the number of set operations on this member")
    public long getLocalSetOperationCount() {
        return localMapStatsDelegate.getLocalStats().getSetOperationCount();
    }

    @ManagedAnnotation("localGetOperationCount")
    @ManagedDescription("number of get operations on this member")
    public long getLocalGetOperationCount() {
        return localMapStatsDelegate.getLocalStats().getGetOperationCount();
    }

    @ManagedAnnotation("localRemoveOperationCount")
    @ManagedDescription("number of remove operations on this member")
    public long getLocalRemoveOperationCount() {
        return localMapStatsDelegate.getLocalStats().getRemoveOperationCount();
    }

    @ManagedAnnotation("localTotalPutLatency")
    @ManagedDescription("the total latency of put operations. To get the average latency, divide to number of puts")
    public long getLocalTotalPutLatency() {
        return localMapStatsDelegate.getLocalStats().getTotalPutLatency();
    }

    @ManagedAnnotation("localTotalSetLatency")
    @ManagedDescription("the total latency of set operations. To get the average latency, divide to number of sets")
    public long getLocalTotalSetLatency() {
        return localMapStatsDelegate.getLocalStats().getTotalSetLatency();
    }

    @ManagedAnnotation("localTotalGetLatency")
    @ManagedDescription("the total latency of get operations. To get the average latency, divide to number of gets")
    public long getLocalTotalGetLatency() {
        return localMapStatsDelegate.getLocalStats().getTotalGetLatency();
    }

    @ManagedAnnotation("localTotalRemoveLatency")
    @ManagedDescription("the total latency of remove operations. To get the average latency, divide to number of gets")
    public long getLocalTotalRemoveLatency() {
        return localMapStatsDelegate.getLocalStats().getTotalRemoveLatency();
    }

    @ManagedAnnotation("localMaxPutLatency")
    @ManagedDescription("the maximum latency of put operations. To get the average latency, divide to number of puts")
    public long getLocalMaxPutLatency() {
        return localMapStatsDelegate.getLocalStats().getMaxPutLatency();
    }

    @ManagedAnnotation("localMaxSetLatency")
    @ManagedDescription("the maximum latency of set operations. To get the average latency, divide to number of sets")
    public long getLocalMaxSetLatency() {
        return localMapStatsDelegate.getLocalStats().getMaxSetLatency();
    }

    @ManagedAnnotation("localMaxGetLatency")
    @ManagedDescription("the maximum latency of get operations. To get the average latency, divide to number of gets")
    public long getLocalMaxGetLatency() {
        return localMapStatsDelegate.getLocalStats().getMaxGetLatency();
    }

    @ManagedAnnotation("localMaxRemoveLatency")
    @ManagedDescription("the maximum latency of remove operations. To get the average latency, divide to number of gets")
    public long getMaxRemoveLatency() {
        return localMapStatsDelegate.getLocalStats().getMaxRemoveLatency();
    }

    @ManagedAnnotation("localEventOperationCount")
    @ManagedDescription("number of events received on this member")
    public long getLocalEventOperationCount() {
        return localMapStatsDelegate.getLocalStats().getEventOperationCount();
    }

    @ManagedAnnotation("localOtherOperationCount")
    @ManagedDescription("the total number of other operations on this member")
    public long getLocalOtherOperationCount() {
        return localMapStatsDelegate.getLocalStats().getOtherOperationCount();
    }

    @ManagedAnnotation("localTotal")
    @ManagedDescription("the total number of operations on this member")
    public long localTotal() {
        return localMapStatsDelegate.getLocalStats().total();
    }

    @ManagedAnnotation("localHeapCost")
    @ManagedDescription("the total heap cost of map, Near Cache and heap cost")
    public long localHeapCost() {
        return localMapStatsDelegate.getLocalStats().getHeapCost();
    }

    @ManagedAnnotation("name")
    @ManagedDescription("name of the map")
    public String getName() {
        return managedObject.getName();
    }

    @ManagedAnnotation("size")
    @ManagedDescription("size of the map")
    public int getSize() {
        return managedObject.size();
    }

    @ManagedAnnotation("config")
    @ManagedDescription("MapConfig")
    public String getConfig() {
        return service.instance.getConfig().findMapConfig(managedObject.getName()).toString();
    }

    @ManagedAnnotation(value = "clear", operation = true)
    @ManagedDescription("Clear Map")
    public void clear() {
        managedObject.clear();
    }
}
