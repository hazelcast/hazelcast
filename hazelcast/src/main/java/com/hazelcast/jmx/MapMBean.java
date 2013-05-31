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
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @ali 2/11/13
 */
@ManagedDescription("IMap")
public class MapMBean extends HazelcastMBean<IMap> {

    private long totalAddedEntryCount;

    private long totalRemovedEntryCount;

    private long totalUpdatedEntryCount;

    private long totalEvictedEntryCount;

    private final EntryListener entryListener;

    private String listenerId;

    protected MapMBean(IMap managedObject, ManagementService service) {
        super(managedObject, service);
        objectName = createObjectName("Map", managedObject.getName());
        entryListener = new EntryListener() {
            public void entryAdded(EntryEvent event) {
                totalAddedEntryCount++;
            }

            public void entryRemoved(EntryEvent event) {
                totalRemovedEntryCount++;
            }

            public void entryUpdated(EntryEvent event) {
                totalUpdatedEntryCount++;
            }

            public void entryEvicted(EntryEvent event) {
                totalEvictedEntryCount++;
            }
        };
        listenerId = managedObject.addEntryListener(entryListener, false);
    }

    public void preDeregister() throws Exception {
        super.preDeregister();
        managedObject.removeEntryListener(listenerId);
    }

    @ManagedAnnotation(value = "clear", operation = true)
    @ManagedDescription("Clear Map")
    public void clear(){
        managedObject.clear();
    }

    @ManagedAnnotation("name")
    @ManagedDescription("name of the map")
    public String getName(){
        return managedObject.getName();
    }

    @ManagedAnnotation("size")
    @ManagedDescription("size of the map")
    public int getSize(){
        return managedObject.size();
    }

    @ManagedAnnotation("config")
    @ManagedDescription("MapConfig")
    public String getConfig(){
        return service.instance.getConfig().getMapConfig(managedObject.getName()).toString();
    }

    @ManagedAnnotation("totalAddedEntryCount")
    public long getTotalAddedEntryCount(){
        return totalAddedEntryCount;
    }

    @ManagedAnnotation("totalRemovedEntryCount")
    public long getTotalRemovedEntryCount() {
        return totalRemovedEntryCount;
    }

    @ManagedAnnotation("totalUpdatedEntryCount")
    public long getTotalUpdatedEntryCount() {
        return totalUpdatedEntryCount;
    }

    @ManagedAnnotation("totalEvictedEntryCount")
    public long getTotalEvictedEntryCount() {
        return totalEvictedEntryCount;
    }

    @ManagedAnnotation(value = "values", operation = true)
    public String values(String query){
        Collection coll;
        if (query != null && !query.isEmpty()){
            Predicate predicate = new SqlPredicate(query);
            coll = managedObject.values(predicate);
        }
        else {
            coll = managedObject.values();
        }
        StringBuilder buf = new StringBuilder();
        if (coll.size() == 0){
            buf.append("Empty");
        }
        else {
            buf.append("[");
            for (Object obj: coll){
                buf.append(obj);
                buf.append(", ");
            }
            buf.replace(buf.length()-1, buf.length(), "]");
        }
        return buf.toString();
    }

    @ManagedAnnotation(value = "entrySet", operation = true)
    public String entrySet(String query){
        Set<Map.Entry> entrySet;
        if (query != null && !query.isEmpty()){
            Predicate predicate = new SqlPredicate(query);
            entrySet = managedObject.entrySet(predicate);
        }
        else {
            entrySet = managedObject.entrySet();
        }

        StringBuilder buf = new StringBuilder();
        if (entrySet.size() == 0){
            buf.append("Empty");
        }
        else {
            buf.append("[");
            for (Map.Entry entry: entrySet){
                buf.append("{key:");
                buf.append(entry.getKey());
                buf.append(", value:");
                buf.append(entry.getValue());
                buf.append("}, ");
            }
            buf.replace(buf.length()-1, buf.length(), "]");
        }
        return buf.toString();
    }


}
