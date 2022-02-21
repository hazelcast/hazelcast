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

import com.hazelcast.multimap.MultiMap;
import com.hazelcast.internal.jmx.suppliers.LocalMultiMapStatsSupplier;
import com.hazelcast.internal.jmx.suppliers.StatsSupplier;
import com.hazelcast.multimap.LocalMultiMapStats;

/**
 * Management bean for {@link MultiMap}
 */
@ManagedDescription("MultiMap")
public class MultiMapMBean extends HazelcastMBean<MultiMap> {

    private final LocalStatsDelegate<LocalMultiMapStats> localMultiMapStatsDelegate;

    protected MultiMapMBean(final MultiMap managedObject, ManagementService service) {
        super(managedObject, service);
        this.objectName = service.createObjectName("MultiMap", managedObject.getName());
        StatsSupplier<LocalMultiMapStats> localMultiMapStatsSupplier = new LocalMultiMapStatsSupplier(managedObject);
        this.localMultiMapStatsDelegate
                = new LocalStatsDelegate<LocalMultiMapStats>(localMultiMapStatsSupplier, updateIntervalSec);
    }

    @ManagedAnnotation("localOwnedEntryCount")
    @ManagedDescription("number of entries owned on this member")
    public long getLocalOwnedEntryCount() {
        return localMultiMapStatsDelegate.getLocalStats().getOwnedEntryCount();
    }

    @ManagedAnnotation("localBackupEntryCount")
    @ManagedDescription("the number of backup entries hold on this member")
    public long getLocalBackupEntryCount() {
        return localMultiMapStatsDelegate.getLocalStats().getBackupEntryCount();
    }

    @ManagedAnnotation("localBackupCount")
    @ManagedDescription("the number of backups per entry on this member")
    public int getLocalBackupCount() {
        return localMultiMapStatsDelegate.getLocalStats().getBackupCount();
    }

    @ManagedAnnotation("localOwnedEntryMemoryCost")
    @ManagedDescription("memory cost (number of bytes) of owned entries on this member")
    public long getLocalOwnedEntryMemoryCost() {
        return localMultiMapStatsDelegate.getLocalStats().getOwnedEntryMemoryCost();
    }

    @ManagedAnnotation("localBackupEntryMemoryCost")
    @ManagedDescription("memory cost (number of bytes) of backup entries on this member.")
    public long getLocalBackupEntryMemoryCost() {
        return localMultiMapStatsDelegate.getLocalStats().getBackupEntryMemoryCost();
    }

    @ManagedAnnotation("localCreationTime")
    @ManagedDescription("the creation time of this map on this member.")
    public long getLocalCreationTime() {
        return localMultiMapStatsDelegate.getLocalStats().getCreationTime();
    }

    @ManagedAnnotation("localLastAccessTime")
    @ManagedDescription("the last access (read) time of the locally owned entries.")
    public long getLocalLastAccessTime() {
        return localMultiMapStatsDelegate.getLocalStats().getLastAccessTime();
    }

    @ManagedAnnotation("localLastUpdateTime")
    @ManagedDescription("the last update time of the locally owned entries.")
    public long getLocalLastUpdateTime() {
        return localMultiMapStatsDelegate.getLocalStats().getLastUpdateTime();
    }

    @ManagedAnnotation("localHits")
    @ManagedDescription("the number of hits (reads) of the locally owned entries.")
    public long getLocalHits() {
        return localMultiMapStatsDelegate.getLocalStats().getHits();
    }

    @ManagedAnnotation("localLockedEntryCount")
    @ManagedDescription("the number of currently locked locally owned keys.")
    public long getLocalLockedEntryCount() {
        return localMultiMapStatsDelegate.getLocalStats().getLockedEntryCount();
    }

    @ManagedAnnotation("localDirtyEntryCount")
    @ManagedDescription("the number of entries that the member owns and are dirty on this member")
    public long getLocalDirtyEntryCount() {
        return localMultiMapStatsDelegate.getLocalStats().getDirtyEntryCount();
    }

    @ManagedAnnotation("localPutOperationCount")
    @ManagedDescription("the number of put operations on this member")
    public long getLocalPutOperationCount() {
        return localMultiMapStatsDelegate.getLocalStats().getPutOperationCount();
    }

    @ManagedAnnotation("localGetOperationCount")
    @ManagedDescription("number of get operations on this member")
    public long getLocalGetOperationCount() {
        return localMultiMapStatsDelegate.getLocalStats().getGetOperationCount();
    }

    @ManagedAnnotation("localRemoveOperationCount")
    @ManagedDescription("number of remove operations on this member")
    public long getLocalRemoveOperationCount() {
        return localMultiMapStatsDelegate.getLocalStats().getRemoveOperationCount();
    }

    @ManagedAnnotation("localTotalPutLatency")
    @ManagedDescription("the total latency of put operations. To get the average latency, divide to number of puts")
    public long getLocalTotalPutLatency() {
        return localMultiMapStatsDelegate.getLocalStats().getTotalPutLatency();
    }

    @ManagedAnnotation("localTotalGetLatency")
    @ManagedDescription("the total latency of get operations. To get the average latency, divide to number of gets")
    public long getLocalTotalGetLatency() {
        return localMultiMapStatsDelegate.getLocalStats().getTotalGetLatency();
    }

    @ManagedAnnotation("localTotalRemoveLatency")
    @ManagedDescription("the total latency of remove operations. To get the average latency, divide to number of gets")
    public long getLocalTotalRemoveLatency() {
        return localMultiMapStatsDelegate.getLocalStats().getTotalRemoveLatency();
    }

    @ManagedAnnotation("localMaxPutLatency")
    @ManagedDescription("the maximum latency of put operations. To get the average latency, divide to number of puts")
    public long getLocalMaxPutLatency() {
        return localMultiMapStatsDelegate.getLocalStats().getMaxPutLatency();
    }

    @ManagedAnnotation("localMaxGetLatency")
    @ManagedDescription("the maximum latency of get operations. To get the average latency, divide to number of gets")
    public long getLocalMaxGetLatency() {
        return localMultiMapStatsDelegate.getLocalStats().getMaxGetLatency();
    }

    @ManagedAnnotation("localMaxRemoveLatency")
    @ManagedDescription("the maximum latency of remove operations. To get the average latency, divide to number of gets")
    public long getMaxRemoveLatency() {
        return localMultiMapStatsDelegate.getLocalStats().getMaxRemoveLatency();
    }

    @ManagedAnnotation("localEventOperationCount")
    @ManagedDescription("number of events received on this member")
    public long getLocalEventOperationCount() {
        return localMultiMapStatsDelegate.getLocalStats().getEventOperationCount();
    }

    @ManagedAnnotation("localOtherOperationCount")
    @ManagedDescription("the total number of other operations on this member")
    public long getLocalOtherOperationCount() {
        return localMultiMapStatsDelegate.getLocalStats().getOtherOperationCount();
    }

    @ManagedAnnotation("localTotal")
    @ManagedDescription("the total number of operations on this member")
    public long localTotal() {
        return localMultiMapStatsDelegate.getLocalStats().total();
    }

    @ManagedAnnotation("name")
    public String getName() {
        return managedObject.getName();
    }

    @ManagedAnnotation(value = "clear", operation = true)
    public void clear() {
        managedObject.clear();
    }

    @ManagedAnnotation("size")
    public int getSize() {
        return managedObject.size();
    }

    @ManagedAnnotation("config")
    @ManagedDescription("MultiMapConfig")
    public String getConfig() {
        return service.instance.getConfig().findMultiMapConfig(managedObject.getName()).toString();
    }
}
