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

import com.hazelcast.internal.jmx.suppliers.LocalReplicatedMapStatsSupplier;
import com.hazelcast.replicatedmap.LocalReplicatedMapStats;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.ReplicatedMapProxy;

/**
 * Management bean for {@link ReplicatedMap}
 */
@ManagedDescription("ReplicatedMap")
public class ReplicatedMapMBean extends HazelcastMBean<ReplicatedMapProxy> {

    private final LocalStatsDelegate<LocalReplicatedMapStats> statsDelegate;

    protected ReplicatedMapMBean(ReplicatedMapProxy managedObject, ManagementService service) {
        super(managedObject, service);
        this.objectName = service.createObjectName("ReplicatedMap", managedObject.getName());
        this.statsDelegate = new LocalStatsDelegate<>(new LocalReplicatedMapStatsSupplier(managedObject), updateIntervalSec);
    }

    @ManagedAnnotation("localOwnedEntryCount")
    @ManagedDescription("number of entries owned on this member")
    public long getLocalOwnedEntryCount() {
        return statsDelegate.getLocalStats().getOwnedEntryCount();
    }

    @ManagedAnnotation("localCreationTime")
    @ManagedDescription("the creation time of this map on this member.")
    public long getLocalCreationTime() {
        return statsDelegate.getLocalStats().getCreationTime();
    }

    @ManagedAnnotation("localLastAccessTime")
    @ManagedDescription("the last access (read) time of the locally owned entries.")
    public long getLocalLastAccessTime() {
        return statsDelegate.getLocalStats().getLastAccessTime();
    }

    @ManagedAnnotation("localLastUpdateTime")
    @ManagedDescription("the last update time of the locally owned entries.")
    public long getLocalLastUpdateTime() {
        return statsDelegate.getLocalStats().getLastUpdateTime();
    }

    @ManagedAnnotation("localHits")
    @ManagedDescription("the number of hits (reads) of the locally owned entries.")
    public long getLocalHits() {
        return statsDelegate.getLocalStats().getHits();
    }

    @ManagedAnnotation("localPutOperationCount")
    @ManagedDescription("the number of put operations on this member")
    public long getLocalPutOperationCount() {
        return statsDelegate.getLocalStats().getPutOperationCount();
    }

    @ManagedAnnotation("localGetOperationCount")
    @ManagedDescription("number of get operations on this member")
    public long getLocalGetOperationCount() {
        return statsDelegate.getLocalStats().getGetOperationCount();
    }

    @ManagedAnnotation("localRemoveOperationCount")
    @ManagedDescription("number of remove operations on this member")
    public long getLocalRemoveOperationCount() {
        return statsDelegate.getLocalStats().getRemoveOperationCount();
    }

    @ManagedAnnotation("localTotalPutLatency")
    @ManagedDescription("the total latency of put operations. To get the average latency, divide to number of puts")
    public long getLocalTotalPutLatency() {
        return statsDelegate.getLocalStats().getTotalPutLatency();
    }

    @ManagedAnnotation("localTotalGetLatency")
    @ManagedDescription("the total latency of get operations. To get the average latency, divide to number of gets")
    public long getLocalTotalGetLatency() {
        return statsDelegate.getLocalStats().getTotalGetLatency();
    }

    @ManagedAnnotation("localTotalRemoveLatency")
    @ManagedDescription("the total latency of remove operations. To get the average latency, divide to number of gets")
    public long getLocalTotalRemoveLatency() {
        return statsDelegate.getLocalStats().getTotalRemoveLatency();
    }

    @ManagedAnnotation("localMaxPutLatency")
    @ManagedDescription("the maximum latency of put operations. To get the average latency, divide to number of puts")
    public long getLocalMaxPutLatency() {
        return statsDelegate.getLocalStats().getMaxPutLatency();
    }

    @ManagedAnnotation("localMaxGetLatency")
    @ManagedDescription("the maximum latency of get operations. To get the average latency, divide to number of gets")
    public long getLocalMaxGetLatency() {
        return statsDelegate.getLocalStats().getMaxGetLatency();
    }

    @ManagedAnnotation("localMaxRemoveLatency")
    @ManagedDescription("the maximum latency of remove operations. To get the average latency, divide to number of gets")
    public long getMaxRemoveLatency() {
        return statsDelegate.getLocalStats().getMaxRemoveLatency();
    }

    @ManagedAnnotation("localEventOperationCount")
    @ManagedDescription("number of events received on this member")
    public long getLocalEventOperationCount() {
        return statsDelegate.getLocalStats().getEventOperationCount();
    }

    @ManagedAnnotation("localOtherOperationCount")
    @ManagedDescription("the total number of other operations on this member")
    public long getLocalOtherOperationCount() {
        return statsDelegate.getLocalStats().getOtherOperationCount();
    }

    @ManagedAnnotation("localTotal")
    @ManagedDescription("the total number of operations on this member")
    public long localTotal() {
        return statsDelegate.getLocalStats().total();
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
    @ManagedDescription("ReplicatedMapConfig")
    public String getConfig() {
        return service.instance.getConfig().findReplicatedMapConfig(managedObject.getName()).toString();
    }

    @ManagedAnnotation(value = "clear", operation = true)
    @ManagedDescription("Clear Map")
    public void clear() {
        managedObject.clear();
    }

}
