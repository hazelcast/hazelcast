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

package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hazelcast.cache.impl.CacheStatistics;
import com.hazelcast.management.SerializableClientEndPoint;
import com.hazelcast.management.SerializableMXBeans;
import com.hazelcast.monitor.LocalCacheStats;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.LocalMultiMapStats;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.monitor.MemberState;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.hazelcast.util.JsonUtil.getArray;
import static com.hazelcast.util.JsonUtil.getObject;
import static com.hazelcast.util.JsonUtil.getString;

public class MemberStateImpl implements MemberState {

    public static final int DEFAULT_PARTITION_COUNT = 271;

    private String address;
    private Map<String, Long> runtimeProps = new HashMap<String, Long>();
    private Map<String, LocalMapStatsImpl> mapStats = new HashMap<String, LocalMapStatsImpl>();
    private Map<String, LocalMultiMapStatsImpl> multiMapStats = new HashMap<String, LocalMultiMapStatsImpl>();
    private Map<String, LocalQueueStatsImpl> queueStats = new HashMap<String, LocalQueueStatsImpl>();
    private Map<String, LocalTopicStatsImpl> topicStats = new HashMap<String, LocalTopicStatsImpl>();
    private Map<String, LocalExecutorStatsImpl> executorStats = new HashMap<String, LocalExecutorStatsImpl>();
    private Map<String, LocalCacheStats> cacheStats = new HashMap<String, LocalCacheStats>();
    private List<Integer> partitions = new ArrayList<Integer>(DEFAULT_PARTITION_COUNT);
    private Collection<SerializableClientEndPoint> clients = new HashSet<SerializableClientEndPoint>();
    private SerializableMXBeans beans = new SerializableMXBeans();

    public MemberStateImpl() {
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("address", address);
        JsonObject mapStatsObject = new JsonObject();
        for (Map.Entry<String, LocalMapStatsImpl> entry : mapStats.entrySet()) {
            mapStatsObject.add(entry.getKey(), entry.getValue().toJson());
        }
        root.add("mapStats", mapStatsObject);
        JsonObject multimapStatsObject = new JsonObject();
        for (Map.Entry<String, LocalMultiMapStatsImpl> entry : multiMapStats.entrySet()) {
            multimapStatsObject.add(entry.getKey(), entry.getValue().toJson());
        }
        root.add("multiMapStats", multimapStatsObject);
        JsonObject queueStatsObject = new JsonObject();
        for (Map.Entry<String, LocalQueueStatsImpl> entry : queueStats.entrySet()) {
            queueStatsObject.add(entry.getKey(), entry.getValue().toJson());
        }
        root.add("queueStats", queueStatsObject);
        JsonObject topicStatsObject = new JsonObject();
        for (Map.Entry<String, LocalTopicStatsImpl> entry : topicStats.entrySet()) {
            topicStatsObject.add(entry.getKey(), entry.getValue().toJson());
        }
        root.add("topicStats", topicStatsObject);
        JsonObject executorStatsObject = new JsonObject();
        for (Map.Entry<String, LocalExecutorStatsImpl> entry : executorStats.entrySet()) {
            executorStatsObject.add(entry.getKey(), entry.getValue().toJson());
        }
        root.add("executorStats", executorStatsObject);
        JsonObject cacheStatsObject = new JsonObject();
        for (Map.Entry<String, LocalCacheStats> entry : cacheStats.entrySet()) {
            cacheStatsObject.add(entry.getKey(), entry.getValue().toJson());
        }
        root.add("cacheStats", cacheStatsObject);
        JsonObject runtimePropsObject = new JsonObject();
        for (Map.Entry<String, Long> entry : runtimeProps.entrySet()) {
            runtimePropsObject.add(entry.getKey(), entry.getValue());
        }
        root.add("runtimeProps", runtimePropsObject);
        JsonArray partitionsArray = new JsonArray();
        for (Integer lsPartition : partitions) {
            partitionsArray.add(lsPartition);
        }
        root.add("partitions", partitionsArray);
        JsonArray clientsArray = new JsonArray();
        for (SerializableClientEndPoint client : clients) {
            clientsArray.add(client.toJson());
        }
        root.add("clients", clientsArray);
        root.add("beans", beans.toJson());
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        address = getString(json, "address");
        final Iterator<JsonObject.Member> mapStatsIterator = getObject(json, "mapStats").iterator();
        while (mapStatsIterator.hasNext()) {
            final JsonObject.Member next = mapStatsIterator.next();
            LocalMapStatsImpl stats = new LocalMapStatsImpl();
            stats.fromJson(next.getValue().asObject());
            mapStats.put(next.getName(), stats);
        }
        final Iterator<JsonObject.Member> multiMapStatsIterator = getObject(json, "multiMapStats").iterator();
        while (multiMapStatsIterator.hasNext()) {
            final JsonObject.Member next = multiMapStatsIterator.next();
            LocalMultiMapStatsImpl stats = new LocalMultiMapStatsImpl();
            stats.fromJson(next.getValue().asObject());
            multiMapStats.put(next.getName(), stats);
        }
        final Iterator<JsonObject.Member> queueStatsIterator = getObject(json, "queueStats").iterator();
        while (queueStatsIterator.hasNext()) {
            final JsonObject.Member next = queueStatsIterator.next();
            LocalQueueStatsImpl stats = new LocalQueueStatsImpl();
            stats.fromJson(next.getValue().asObject());
            queueStats.put(next.getName(), stats);
        }
        final Iterator<JsonObject.Member> topicStatsIterator = getObject(json, "topicStats").iterator();
        while (topicStatsIterator.hasNext()) {
            final JsonObject.Member next = topicStatsIterator.next();
            LocalTopicStatsImpl stats = new LocalTopicStatsImpl();
            stats.fromJson(next.getValue().asObject());
            topicStats.put(next.getName(), stats);
        }
        final Iterator<JsonObject.Member> executorStatsIterator = getObject(json, "executorStats").iterator();
        while (executorStatsIterator.hasNext()) {
            final JsonObject.Member next = executorStatsIterator.next();
            LocalExecutorStatsImpl stats = new LocalExecutorStatsImpl();
            stats.fromJson(next.getValue().asObject());
            executorStats.put(next.getName(), stats);
        }
        final Iterator<JsonObject.Member> cacheStatsIterator = getObject(json, "cacheStats", new JsonObject()).iterator();
        while (cacheStatsIterator.hasNext()) {
            final JsonObject.Member next = cacheStatsIterator.next();
            LocalCacheStats stats = new LocalCacheStatsImpl();
            stats.fromJson(next.getValue().asObject());
            cacheStats.put(next.getName(), stats);
        }
        final Iterator<JsonObject.Member> propsIterator = getObject(json, "runtimeProps").iterator();
        while (propsIterator.hasNext()) {
            final JsonObject.Member next = propsIterator.next();
            runtimeProps.put(next.getName(), next.getValue().asLong());
        }
        final JsonArray jsonPartitions = getArray(json, "partitions");
        for (JsonValue jsonPartition : jsonPartitions) {
            partitions.add(jsonPartition.asInt());
        }
        final JsonArray jsonClients = getArray(json, "clients");
        for (JsonValue jsonClient : jsonClients) {
            final SerializableClientEndPoint client = new SerializableClientEndPoint();
            client.fromJson(jsonClient.asObject());
            clients.add(client);
        }
        beans = new SerializableMXBeans();
        beans.fromJson(getObject(json, "beans"));
    }

    public void clearPartitions() {
        partitions.clear();
    }

    public void addPartition(int partitionId) {
        partitions.add(partitionId);
    }


    @Override
    public List<Integer> getPartitions() {
        return partitions;
    }


    @Override
    public Map<String, Long> getRuntimeProps() {
        return runtimeProps;
    }

    public void setRuntimeProps(Map<String, Long> runtimeProps) {
        this.runtimeProps = runtimeProps;
    }

    @Override
    public LocalMapStats getLocalMapStats(String mapName) {
        return mapStats.get(mapName);
    }

    @Override
    public LocalMultiMapStats getLocalMultiMapStats(String mapName) {
        return multiMapStats.get(mapName);
    }

    @Override
    public LocalQueueStats getLocalQueueStats(String queueName) {
        return queueStats.get(queueName);
    }

    @Override
    public LocalTopicStats getLocalTopicStats(String topicName) {
        return topicStats.get(topicName);
    }

    @Override
    public LocalExecutorStats getLocalExecutorStats(String executorName) {
        return executorStats.get(executorName);
    }

    @Override
    public LocalCacheStats getLocalCacheStats(String cacheName) {
        return cacheStats.get(cacheName);
    }

    @Override
    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public void putLocalMapStats(String name, LocalMapStatsImpl localMapStats) {
        mapStats.put(name, localMapStats);
    }

    public void putLocalMultiMapStats(String name, LocalMultiMapStatsImpl localMultiMapStats) {
        multiMapStats.put(name, localMultiMapStats);
    }

    public void putLocalQueueStats(String name, LocalQueueStatsImpl localQueueStats) {
        queueStats.put(name, localQueueStats);
    }

    public void putLocalTopicStats(String name, LocalTopicStatsImpl localTopicStats) {
        topicStats.put(name, localTopicStats);
    }

    public void putLocalExecutorStats(String name, LocalExecutorStatsImpl localExecutorStats) {
        executorStats.put(name, localExecutorStats);
    }

    public void putLocalCacheStats(String name, LocalCacheStats localCacheStats) {
        cacheStats.put(name, localCacheStats);
    }

    public Collection<SerializableClientEndPoint> getClients() {
        return clients;
    }

    @Override
    public SerializableMXBeans getMXBeans() {
        return beans;
    }

    public void setBeans(SerializableMXBeans beans) {
        this.beans = beans;
    }

    public void setClients(Collection<SerializableClientEndPoint> clients) {
        this.clients = clients;
    }

    @Override
    public int hashCode() {
        int result = address != null ? address.hashCode() : 0;
        result = 31 * result + (mapStats != null ? mapStats.hashCode() : 0);
        result = 31 * result + (multiMapStats != null ? multiMapStats.hashCode() : 0);
        result = 31 * result + (queueStats != null ? queueStats.hashCode() : 0);
        result = 31 * result + (topicStats != null ? topicStats.hashCode() : 0);
        result = 31 * result + (executorStats != null ? executorStats.hashCode() : 0);
        result = 31 * result + (cacheStats != null ? cacheStats.hashCode() : 0);
        result = 31 * result + (partitions != null ? partitions.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MemberStateImpl that = (MemberStateImpl) o;

        if (address != null ? !address.equals(that.address) : that.address != null) {
            return false;
        }
        if (executorStats != null ? !executorStats.equals(that.executorStats) : that.executorStats != null) {
            return false;
        }
        if (mapStats != null ? !mapStats.equals(that.mapStats) : that.mapStats != null) {
            return false;
        }
        if (multiMapStats != null ? !multiMapStats.equals(that.multiMapStats) : that.multiMapStats != null) {
            return false;
        }
        if (partitions != null ? !partitions.equals(that.partitions) : that.partitions != null) {
            return false;
        }
        if (queueStats != null ? !queueStats.equals(that.queueStats) : that.queueStats != null) {
            return false;
        }
        if (runtimeProps != null ? !runtimeProps.equals(that.runtimeProps) : that.runtimeProps != null) {
            return false;
        }
        if (topicStats != null ? !topicStats.equals(that.topicStats) : that.topicStats != null) {
            return false;
        }
        if (cacheStats != null ? !cacheStats.equals(that.cacheStats) : that.cacheStats != null) {
            return false;
        }

        return true;
    }


    @Override
    public String toString() {
        return "MemberStateImpl{"
                + "address=" + address
                + ", runtimeProps=" + runtimeProps
                + ", mapStats=" + mapStats
                + ", multiMapStats=" + multiMapStats
                + ", queueStats=" + queueStats
                + ", topicStats=" + topicStats
                + ", executorStats=" + executorStats
                + ", cacheStats=" + cacheStats
                + ", partitions=" + partitions
                + '}';
    }
}