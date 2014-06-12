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

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MapWideEvent;
import com.hazelcast.replicatedmap.ReplicatedMapProxy;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

@ManagedDescription("ReplicatedMap")
public class ReplicatedMapMBean extends HazelcastMBean<ReplicatedMapProxy> {

    private final AtomicLong totalAddedEntryCount = new AtomicLong();
    private final AtomicLong totalRemovedEntryCount = new AtomicLong();
    private final AtomicLong totalUpdatedEntryCount = new AtomicLong();
    private final String listenerId;

    protected ReplicatedMapMBean(ReplicatedMapProxy managedObject, ManagementService service) {
        super(managedObject, service);
        objectName = service.createObjectName("ReplicatedMap", managedObject.getName());

        //todo: using the event system to register number of adds/remove is an very expensive price to pay.
        EntryListener entryListener = new EntryListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                totalAddedEntryCount.incrementAndGet();
            }

            @Override
            public void entryRemoved(EntryEvent event) {
                totalRemovedEntryCount.incrementAndGet();
            }

            @Override
            public void entryUpdated(EntryEvent event) {
                totalUpdatedEntryCount.incrementAndGet();
            }

            @Override
            public void entryEvicted(EntryEvent event) {
            }

            @Override
            public void evictedAll(MapWideEvent event) {

            }
        };
        listenerId = managedObject.addEntryListener(entryListener, false);
    }

    @Override
    public void preDeregister() throws Exception {
        super.preDeregister();
        try {
            managedObject.removeEntryListener(listenerId);
        } catch (Exception ignored) {
        }
    }

    @ManagedAnnotation("localOwnedEntryCount")
    @ManagedDescription("number of entries owned on this member")
    public long getLocalOwnedEntryCount() {
        return managedObject.getReplicatedMapStats().getOwnedEntryCount();
    }

    @ManagedAnnotation("localCreationTime")
    @ManagedDescription("the creation time of this map on this member.")
    public long getLocalCreationTime() {
        return managedObject.getReplicatedMapStats().getCreationTime();
    }

    @ManagedAnnotation("localLastAccessTime")
    @ManagedDescription("the last access (read) time of the locally owned entries.")
    public long getLocalLastAccessTime() {
        return managedObject.getReplicatedMapStats().getLastAccessTime();
    }

    @ManagedAnnotation("localLastUpdateTime")
    @ManagedDescription("the last update time of the locally owned entries.")
    public long getLocalLastUpdateTime() {
        return managedObject.getReplicatedMapStats().getLastUpdateTime();
    }

    @ManagedAnnotation("localHits")
    @ManagedDescription("the number of hits (reads) of the locally owned entries.")
    public long getLocalHits() {
        return managedObject.getReplicatedMapStats().getHits();
    }

    @ManagedAnnotation("localPutOperationCount")
    @ManagedDescription("the number of put operations on this member")
    public long getLocalPutOperationCount() {
        return managedObject.getReplicatedMapStats().getPutOperationCount();
    }

    @ManagedAnnotation("localGetOperationCount")
    @ManagedDescription("number of get operations on this member")
    public long getLocalGetOperationCount() {
        return managedObject.getReplicatedMapStats().getGetOperationCount();
    }

    @ManagedAnnotation("localRemoveOperationCount")
    @ManagedDescription("number of remove operations on this member")
    public long getLocalRemoveOperationCount() {
        return managedObject.getReplicatedMapStats().getRemoveOperationCount();
    }

    @ManagedAnnotation("localTotalPutLatency")
    @ManagedDescription("the total latency of put operations. To get the average latency, divide to number of puts")
    public long getLocalTotalPutLatency() {
        return managedObject.getReplicatedMapStats().getTotalPutLatency();
    }

    @ManagedAnnotation("localTotalGetLatency")
    @ManagedDescription("the total latency of get operations. To get the average latency, divide to number of gets")
    public long getLocalTotalGetLatency() {
        return managedObject.getReplicatedMapStats().getTotalGetLatency();
    }

    @ManagedAnnotation("localTotalRemoveLatency")
    @ManagedDescription("the total latency of remove operations. To get the average latency, divide to number of gets")
    public long getLocalTotalRemoveLatency() {
        return managedObject.getReplicatedMapStats().getTotalRemoveLatency();
    }

    @ManagedAnnotation("localMaxPutLatency")
    @ManagedDescription("the maximum latency of put operations. To get the average latency, divide to number of puts")
    public long getLocalMaxPutLatency() {
        return managedObject.getReplicatedMapStats().getMaxPutLatency();
    }

    @ManagedAnnotation("localMaxGetLatency")
    @ManagedDescription("the maximum latency of get operations. To get the average latency, divide to number of gets")
    public long getLocalMaxGetLatency() {
        return managedObject.getReplicatedMapStats().getMaxGetLatency();
    }

    @ManagedAnnotation("localMaxRemoveLatency")
    @ManagedDescription("the maximum latency of remove operations. To get the average latency, divide to number of gets")
    public long getMaxRemoveLatency() {
        return managedObject.getReplicatedMapStats().getMaxRemoveLatency();
    }

    @ManagedAnnotation("localEventOperationCount")
    @ManagedDescription("number of events received on this member")
    public long getLocalEventOperationCount() {
        return managedObject.getReplicatedMapStats().getEventOperationCount();
    }

    @ManagedAnnotation("localReplicationEventCount")
    @ManagedDescription("number of replication events received on this member")
    public long getLocalReplicationEventCount() {
        return managedObject.getReplicatedMapStats().getReplicationEventCount();
    }

    @ManagedAnnotation("localOtherOperationCount")
    @ManagedDescription("the total number of other operations on this member")
    public long getLocalOtherOperationCount() {
        return managedObject.getReplicatedMapStats().getOtherOperationCount();
    }

    @ManagedAnnotation("localTotal")
    @ManagedDescription("the total number of operations on this member")
    public long localTotal() {
        return managedObject.getReplicatedMapStats().total();
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

    @ManagedAnnotation("totalAddedEntryCount")
    public long getTotalAddedEntryCount() {
        return totalAddedEntryCount.get();
    }

    @ManagedAnnotation("totalRemovedEntryCount")
    public long getTotalRemovedEntryCount() {
        return totalRemovedEntryCount.get();
    }

    @ManagedAnnotation("totalUpdatedEntryCount")
    public long getTotalUpdatedEntryCount() {
        return totalUpdatedEntryCount.get();
    }

    @ManagedAnnotation(value = "clear", operation = true)
    @ManagedDescription("Clear Map")
    public void clear() {
        managedObject.clear();
    }

    @ManagedAnnotation(value = "values", operation = true)
    public String values() {
        Collection coll = managedObject.values();
        StringBuilder buf = new StringBuilder();
        if (coll.size() == 0) {
            buf.append("Empty");
        } else {
            buf.append("[");
            for (Object obj : coll) {
                buf.append(obj);
                buf.append(", ");
            }
            buf.replace(buf.length() - 1, buf.length(), "]");
        }
        return buf.toString();
    }

    @ManagedAnnotation(value = "entrySet", operation = true)
    public String entrySet() {
        Set<Map.Entry> entrySet = managedObject.entrySet();

        StringBuilder buf = new StringBuilder();
        if (entrySet.size() == 0) {
            buf.append("Empty");
        } else {
            buf.append("[");
            for (Map.Entry entry : entrySet) {
                buf.append("{key:");
                buf.append(entry.getKey());
                buf.append(", value:");
                buf.append(entry.getValue());
                buf.append("}, ");
            }
            buf.replace(buf.length() - 1, buf.length(), "]");
        }
        return buf.toString();
    }
}
