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

import com.hazelcast.management.SerializableClientEndPoint;
<<<<<<< HEAD
import com.hazelcast.management.SerializableMXBeans;
=======
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
>>>>>>> management center json communication initial commit
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.LocalMultiMapStats;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.monitor.MemberState;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class MemberStateImpl implements MemberState {

    public static final int DEFAULT_PARTITION_COUNT = 271;

<<<<<<< HEAD
    private Address address = new Address();
    private Map<String, Long> runtimeProps = new HashMap<String, Long>();
    private Map<String, LocalMapStatsImpl> mapStats = new HashMap<String, LocalMapStatsImpl>();
    private Map<String, LocalMultiMapStatsImpl> multiMapStats = new HashMap<String, LocalMultiMapStatsImpl>();
    private Map<String, LocalQueueStatsImpl> queueStats = new HashMap<String, LocalQueueStatsImpl>();
    private Map<String, LocalTopicStatsImpl> topicStats = new HashMap<String, LocalTopicStatsImpl>();
    private Map<String, LocalExecutorStatsImpl> executorStats = new HashMap<String, LocalExecutorStatsImpl>();
    private List<Integer> partitions = new ArrayList<Integer>(DEFAULT_PARTITION_COUNT);
    private Collection<SerializableClientEndPoint> clients = new HashSet<SerializableClientEndPoint>();
    private SerializableMXBeans beans = new SerializableMXBeans();
=======
    String address;
    Map<String, Long> runtimeProps = new HashMap<String, Long>();
    Map<String, LocalMapStatsImpl> mapStats = new HashMap<String, LocalMapStatsImpl>();
    Map<String, LocalMultiMapStatsImpl> multiMapStats = new HashMap<String, LocalMultiMapStatsImpl>();
    Map<String, LocalQueueStatsImpl> queueStats = new HashMap<String, LocalQueueStatsImpl>();
    Map<String, LocalTopicStatsImpl> topicStats = new HashMap<String, LocalTopicStatsImpl>();
    Map<String, LocalExecutorStatsImpl> executorStats = new HashMap<String, LocalExecutorStatsImpl>();
    List<Integer> partitions = new ArrayList<Integer>(DEFAULT_PARTITION_COUNT);
    Collection<SerializableClientEndPoint> clients = new HashSet<SerializableClientEndPoint>();
>>>>>>> management center json communication initial commit

    public MemberStateImpl() {
    }

    @Override
    public JsonValue toJson() {
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
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {

    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(address);
        out.writeInt(mapStats.size());
        for (Map.Entry<String, LocalMapStatsImpl> entry : mapStats.entrySet()) {
            out.writeUTF(entry.getKey());
            entry.getValue().writeData(out);
        }
        out.writeInt(multiMapStats.size());
        for (Map.Entry<String, LocalMultiMapStatsImpl> entry : multiMapStats.entrySet()) {
            out.writeUTF(entry.getKey());
            entry.getValue().writeData(out);
        }
        out.writeInt(queueStats.size());
        for (Map.Entry<String, LocalQueueStatsImpl> entry : queueStats.entrySet()) {
            out.writeUTF(entry.getKey());
            entry.getValue().writeData(out);
        }
        out.writeInt(topicStats.size());
        for (Map.Entry<String, LocalTopicStatsImpl> entry : topicStats.entrySet()) {
            out.writeUTF(entry.getKey());
            entry.getValue().writeData(out);
        }
        out.writeInt(executorStats.size());
        for (Map.Entry<String, LocalExecutorStatsImpl> entry : executorStats.entrySet()) {
            out.writeUTF(entry.getKey());
            entry.getValue().writeData(out);
        }

        out.writeInt(runtimeProps.size());
        for (Map.Entry<String, Long> entry : runtimeProps.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeLong(entry.getValue());
        }
        out.writeInt(partitions.size());
        for (Integer lsPartition : partitions) {
            out.writeInt(lsPartition);
        }
        out.writeInt(clients.size());
        for (SerializableClientEndPoint client : clients) {
            client.writeData(out);
        }
        beans.writeData(out);
    }

    public void readData(ObjectDataInput in) throws IOException {
        address = in.readUTF();
        DataSerializable impl;
        String name;
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            impl = new LocalMapStatsImpl();
            impl.readData(in);
            mapStats.put(name, (LocalMapStatsImpl) impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            impl = new LocalMultiMapStatsImpl();
            impl.readData(in);
            multiMapStats.put(name, (LocalMultiMapStatsImpl) impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            impl = new LocalQueueStatsImpl();
            impl.readData(in);
            queueStats.put(name, (LocalQueueStatsImpl) impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            impl = new LocalTopicStatsImpl();
            impl.readData(in);
            topicStats.put(name, (LocalTopicStatsImpl) impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            impl = new LocalExecutorStatsImpl();
            impl.readData(in);
            executorStats.put(name, (LocalExecutorStatsImpl) impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            runtimeProps.put(name, in.readLong());
        }
        for (int i = in.readInt(); i > 0; i--) {
            partitions.add(in.readInt());
        }
        for (int i = in.readInt(); i > 0; i--) {
            SerializableClientEndPoint ci = new SerializableClientEndPoint();
            ci.readData(in);
            clients.add(ci);
        }
        beans.readData(in);
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
                + ", partitions=" + partitions
                + '}';
    }
}
