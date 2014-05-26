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
import com.hazelcast.management.SerializableMXBeans;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.LocalMultiMapStats;
import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.monitor.MemberState;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class MemberStateImpl implements MemberState {

    public static final int DEFAULT_PARTITION_COUNT = 271;

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

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        address.writeData(out);
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

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        address.readData(in);
        for (int i = in.readInt(); i > 0; i--) {
            String name = in.readUTF();
            LocalMapStatsImpl impl = new LocalMapStatsImpl();
            impl.readData(in);
            mapStats.put(name, impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            String name = in.readUTF();
            LocalMultiMapStatsImpl impl = new LocalMultiMapStatsImpl();
            impl.readData(in);
            multiMapStats.put(name, impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            String name = in.readUTF();
            LocalQueueStatsImpl impl = new LocalQueueStatsImpl();
            impl.readData(in);
            queueStats.put(name, impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            String name = in.readUTF();
            LocalTopicStatsImpl impl = new LocalTopicStatsImpl();
            impl.readData(in);
            topicStats.put(name, impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            String name = in.readUTF();
            LocalExecutorStatsImpl impl = new LocalExecutorStatsImpl();
            impl.readData(in);
            executorStats.put(name, impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            String name = in.readUTF();
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
    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
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
