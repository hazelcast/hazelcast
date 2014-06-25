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
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.util.EmptyStatement.ignore;

/**
 * Management bean for {@link com.hazelcast.core.IMap}
 */
@ManagedDescription("IMap")
public class MapMBean extends HazelcastMBean<IMap> {

    private final AtomicLong totalAddedEntryCount = new AtomicLong();
    private final AtomicLong totalRemovedEntryCount = new AtomicLong();
    private final AtomicLong totalUpdatedEntryCount = new AtomicLong();
    private final AtomicLong totalEvictedEntryCount = new AtomicLong();
    private final String listenerId;

    protected MapMBean(IMap managedObject, ManagementService service) {
        super(managedObject, service);
        objectName = service.createObjectName("IMap", managedObject.getName());
        //todo: using the event system to register number of adds/remove is an very expensive price to pay.
        EntryListener entryListener = new EntryListener() {
            public void entryAdded(EntryEvent event) {
                totalAddedEntryCount.incrementAndGet();
            }

            public void entryRemoved(EntryEvent event) {
                totalRemovedEntryCount.incrementAndGet();
            }

            public void entryUpdated(EntryEvent event) {
                totalUpdatedEntryCount.incrementAndGet();
            }

            public void entryEvicted(EntryEvent event) {
                totalEvictedEntryCount.incrementAndGet();
            }

            @Override
            public void mapEvicted(MapEvent event) {
                totalEvictedEntryCount.addAndGet(event.getNumberOfEntriesAffected());
            }

            @Override
            public void mapCleared(MapEvent event) {
                //TODO should I add totalClearedEntryCount?
                totalRemovedEntryCount.addAndGet(event.getNumberOfEntriesAffected());
            }
        };
        listenerId = managedObject.addEntryListener(entryListener, false);
    }

    public void preDeregister() throws Exception {
        super.preDeregister();
        try {
            managedObject.removeEntryListener(listenerId);
        } catch (Exception ignored) {
            ignore(ignored);
        }
    }

    @ManagedAnnotation("localOwnedEntryCount")
    @ManagedDescription("number of entries owned on this member")
    public long getLocalOwnedEntryCount() {
        return managedObject.getLocalMapStats().getOwnedEntryCount();
    }

    @ManagedAnnotation("localBackupEntryCount")
    @ManagedDescription("the number of backup entries hold on this member")
    public long getLocalBackupEntryCount() {
        return managedObject.getLocalMapStats().getBackupEntryCount();
    }

    @ManagedAnnotation("localBackupCount")
    @ManagedDescription("the number of backups per entry on this member")
    public int getLocalBackupCount() {
        return managedObject.getLocalMapStats().getBackupCount();
    }

    @ManagedAnnotation("localOwnedEntryMemoryCost")
    @ManagedDescription("memory cost (number of bytes) of owned entries on this member")
    public long getLocalOwnedEntryMemoryCost() {
        return managedObject.getLocalMapStats().getOwnedEntryMemoryCost();
    }

    @ManagedAnnotation("localBackupEntryMemoryCost")
    @ManagedDescription("memory cost (number of bytes) of backup entries on this member.")
    public long getLocalBackupEntryMemoryCost() {
        return managedObject.getLocalMapStats().getBackupEntryMemoryCost();
    }

    @ManagedAnnotation("localCreationTime")
    @ManagedDescription("the creation time of this map on this member.")
    public long getLocalCreationTime() {
        return managedObject.getLocalMapStats().getCreationTime();
    }

    @ManagedAnnotation("localLastAccessTime")
    @ManagedDescription("the last access (read) time of the locally owned entries.")
    public long getLocalLastAccessTime() {
        return managedObject.getLocalMapStats().getLastAccessTime();
    }

    @ManagedAnnotation("localLastUpdateTime")
    @ManagedDescription("the last update time of the locally owned entries.")
    public long getLocalLastUpdateTime() {
        return managedObject.getLocalMapStats().getLastUpdateTime();
    }

    @ManagedAnnotation("localHits")
    @ManagedDescription("the number of hits (reads) of the locally owned entries.")
    public long getLocalHits() {
        return managedObject.getLocalMapStats().getHits();
    }

    @ManagedAnnotation("localLockedEntryCount")
    @ManagedDescription("the number of currently locked locally owned keys.")
    public long getLocalLockedEntryCount() {
        return managedObject.getLocalMapStats().getLockedEntryCount();
    }

    @ManagedAnnotation("localDirtyEntryCount")
    @ManagedDescription("the number of entries that the member owns and are dirty on this member")
    public long getLocalDirtyEntryCount() {
        return managedObject.getLocalMapStats().getDirtyEntryCount();
    }

    @ManagedAnnotation("localPutOperationCount")
    @ManagedDescription("the number of put operations on this member")
    public long getLocalPutOperationCount() {
        return managedObject.getLocalMapStats().getPutOperationCount();
    }

    @ManagedAnnotation("localGetOperationCount")
    @ManagedDescription("number of get operations on this member")
    public long getLocalGetOperationCount() {
        return managedObject.getLocalMapStats().getGetOperationCount();
    }

    @ManagedAnnotation("localRemoveOperationCount")
    @ManagedDescription("number of remove operations on this member")
    public long getLocalRemoveOperationCount() {
        return managedObject.getLocalMapStats().getRemoveOperationCount();
    }

    @ManagedAnnotation("localTotalPutLatency")
    @ManagedDescription("the total latency of put operations. To get the average latency, divide to number of puts")
    public long getLocalTotalPutLatency() {
        return managedObject.getLocalMapStats().getTotalPutLatency();
    }

    @ManagedAnnotation("localTotalGetLatency")
    @ManagedDescription("the total latency of get operations. To get the average latency, divide to number of gets")
    public long getLocalTotalGetLatency() {
        return managedObject.getLocalMapStats().getTotalGetLatency();
    }

    @ManagedAnnotation("localTotalRemoveLatency")
    @ManagedDescription("the total latency of remove operations. To get the average latency, divide to number of gets")
    public long getLocalTotalRemoveLatency() {
        return managedObject.getLocalMapStats().getTotalRemoveLatency();
    }

    @ManagedAnnotation("localMaxPutLatency")
    @ManagedDescription("the maximum latency of put operations. To get the average latency, divide to number of puts")
    public long getLocalMaxPutLatency() {
        return managedObject.getLocalMapStats().getMaxPutLatency();
    }

    @ManagedAnnotation("localMaxGetLatency")
    @ManagedDescription("the maximum latency of get operations. To get the average latency, divide to number of gets")
    public long getLocalMaxGetLatency() {
        return managedObject.getLocalMapStats().getMaxGetLatency();
    }

    @ManagedAnnotation("localMaxRemoveLatency")
    @ManagedDescription("the maximum latency of remove operations. To get the average latency, divide to number of gets")
    public long getMaxRemoveLatency() {
        return managedObject.getLocalMapStats().getMaxRemoveLatency();
    }

    @ManagedAnnotation("localEventOperationCount")
    @ManagedDescription("number of events received on this member")
    public long getLocalEventOperationCount() {
        return managedObject.getLocalMapStats().getEventOperationCount();
    }

    @ManagedAnnotation("localOtherOperationCount")
    @ManagedDescription("the total number of other operations on this member")
    public long getLocalOtherOperationCount() {
        return managedObject.getLocalMapStats().getOtherOperationCount();
    }

    @ManagedAnnotation("localTotal")
    @ManagedDescription("the total number of operations on this member")
    public long localTotal() {
        return managedObject.getLocalMapStats().total();
    }

    @ManagedAnnotation("localHeapCost")
    @ManagedDescription("the total heap cost of map, near cache and heap cost")
    public long localHeapCost() {
        return managedObject.getLocalMapStats().getHeapCost();
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

    @ManagedAnnotation("totalEvictedEntryCount")
    public long getTotalEvictedEntryCount() {
        return totalEvictedEntryCount.get();
    }

    @ManagedAnnotation(value = "clear", operation = true)
    @ManagedDescription("Clear Map")
    public void clear() {
        managedObject.clear();
    }

    @ManagedAnnotation(value = "values", operation = true)
    public String values(String query) {
        Collection coll;
        if (query != null && !query.isEmpty()) {
            Predicate predicate = new SqlPredicate(query);
            coll = managedObject.values(predicate);
        } else {
            coll = managedObject.values();
        }
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
    public String entrySet(String query) {
        Set<Map.Entry> entrySet;
        if (query != null && !query.isEmpty()) {
            Predicate predicate = new SqlPredicate(query);
            entrySet = managedObject.entrySet(predicate);
        } else {
            entrySet = managedObject.entrySet();
        }

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
