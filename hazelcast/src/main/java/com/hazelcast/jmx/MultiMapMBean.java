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

package com.hazelcast.jmx;

import com.hazelcast.core.MultiMap;

@ManagedDescription("MultiMap")
public class MultiMapMBean extends HazelcastMBean<MultiMap> {

    protected MultiMapMBean(MultiMap managedObject, ManagementService service) {
        super(managedObject, service);
        objectName = service.createObjectName("MultiMap", managedObject.getName());
    }

    @ManagedAnnotation("localOwnedEntryCount")
    @ManagedDescription("number of entries owned on this member")
    public long getLocalOwnedEntryCount() {
        return managedObject.getLocalMultiMapStats().getOwnedEntryCount();
    }

    @ManagedAnnotation("localBackupEntryCount")
    @ManagedDescription("the number of backup entries hold on this member")
    public long getLocalBackupEntryCount() {
        return managedObject.getLocalMultiMapStats().getBackupEntryCount();
    }

    @ManagedAnnotation("localBackupCount")
    @ManagedDescription("the number of backups per entry on this member")
    public int getLocalBackupCount() {
        return managedObject.getLocalMultiMapStats().getBackupCount();
    }

    @ManagedAnnotation("localOwnedEntryMemoryCost")
    @ManagedDescription("memory cost (number of bytes) of owned entries on this member")
    public long getLocalOwnedEntryMemoryCost() {
        return managedObject.getLocalMultiMapStats().getOwnedEntryMemoryCost();
    }

    @ManagedAnnotation("localBackupEntryMemoryCost")
    @ManagedDescription("memory cost (number of bytes) of backup entries on this member.")
    public long getLocalBackupEntryMemoryCost() {
        return managedObject.getLocalMultiMapStats().getBackupEntryMemoryCost();
    }

    @ManagedAnnotation("localCreationTime")
    @ManagedDescription("the creation time of this map on this member.")
    public long getLocalCreationTime() {
        return managedObject.getLocalMultiMapStats().getCreationTime();
    }

    @ManagedAnnotation("localLastAccessTime")
    @ManagedDescription("the last access (read) time of the locally owned entries.")
    public long getLocalLastAccessTime() {
        return managedObject.getLocalMultiMapStats().getLastAccessTime();
    }

    @ManagedAnnotation("localLastUpdateTime")
    @ManagedDescription("the last update time of the locally owned entries.")
    public long getLocalLastUpdateTime() {
        return managedObject.getLocalMultiMapStats().getLastUpdateTime();
    }

    @ManagedAnnotation("localHits")
    @ManagedDescription("the number of hits (reads) of the locally owned entries.")
    public long getLocalHits() {
        return managedObject.getLocalMultiMapStats().getHits();
    }

    @ManagedAnnotation("localLockedEntryCount")
    @ManagedDescription("the number of currently locked locally owned keys.")
    public long getLocalLockedEntryCount() {
        return managedObject.getLocalMultiMapStats().getLockedEntryCount();
    }

    @ManagedAnnotation("localDirtyEntryCount")
    @ManagedDescription("the number of entries that the member owns and are dirty on this member")
    public long getLocalDirtyEntryCount() {
        return managedObject.getLocalMultiMapStats().getDirtyEntryCount();
    }

    @ManagedAnnotation("localPutOperationCount")
    @ManagedDescription("the number of put operations on this member")
    public long getLocalPutOperationCount() {
        return managedObject.getLocalMultiMapStats().getPutOperationCount();
    }

    @ManagedAnnotation("localGetOperationCount")
    @ManagedDescription("number of get operations on this member")
    public long getLocalGetOperationCount() {
        return managedObject.getLocalMultiMapStats().getGetOperationCount();
    }

    @ManagedAnnotation("localRemoveOperationCount")
    @ManagedDescription("number of remove operations on this member")
    public long getLocalRemoveOperationCount() {
        return managedObject.getLocalMultiMapStats().getRemoveOperationCount();
    }

    @ManagedAnnotation("localTotalPutLatency")
    @ManagedDescription("the total latency of put operations. To get the average latency, divide to number of puts")
    public long getLocalTotalPutLatency() {
        return managedObject.getLocalMultiMapStats().getTotalPutLatency();
    }

    @ManagedAnnotation("localTotalGetLatency")
    @ManagedDescription("the total latency of get operations. To get the average latency, divide to number of gets")
    public long getLocalTotalGetLatency() {
        return managedObject.getLocalMultiMapStats().getTotalGetLatency();
    }

    @ManagedAnnotation("localTotalRemoveLatency")
    @ManagedDescription("the total latency of remove operations. To get the average latency, divide to number of gets")
    public long getLocalTotalRemoveLatency() {
        return managedObject.getLocalMultiMapStats().getTotalRemoveLatency();
    }

    @ManagedAnnotation("localMaxPutLatency")
    @ManagedDescription("the maximum latency of put operations. To get the average latency, divide to number of puts")
    public long getLocalMaxPutLatency() {
        return managedObject.getLocalMultiMapStats().getMaxPutLatency();
    }

    @ManagedAnnotation("localMaxGetLatency")
    @ManagedDescription("the maximum latency of get operations. To get the average latency, divide to number of gets")
    public long getLocalMaxGetLatency() {
        return managedObject.getLocalMultiMapStats().getMaxGetLatency();
    }

    @ManagedAnnotation("localMaxRemoveLatency")
    @ManagedDescription("the maximum latency of remove operations. To get the average latency, divide to number of gets")
    public long getMaxRemoveLatency() {
        return managedObject.getLocalMultiMapStats().getMaxRemoveLatency();
    }

    @ManagedAnnotation("localEventOperationCount")
    @ManagedDescription("number of events received on this member")
    public long getLocalEventOperationCount() {
        return managedObject.getLocalMultiMapStats().getEventOperationCount();
    }

    @ManagedAnnotation("localOtherOperationCount")
    @ManagedDescription("the total number of other operations on this member")
    public long getLocalOtherOperationCount() {
        return managedObject.getLocalMultiMapStats().getOtherOperationCount();
    }

    @ManagedAnnotation("localTotal")
    @ManagedDescription("the total number of operations on this member")
    public long localTotal() {
        return managedObject.getLocalMultiMapStats().total();
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
}
