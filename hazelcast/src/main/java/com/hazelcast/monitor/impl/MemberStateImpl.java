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

import com.hazelcast.monitor.*;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemberStateImpl implements MemberState {

    Address address = new Address();
    Map<String, Long> runtimeProps = new HashMap<String, Long>();
    Map<String, LocalMapStatsImpl> mapStats = new HashMap<String, LocalMapStatsImpl>();
    Map<String, LocalMultiMapStatsImpl> multiMapStats = new HashMap<String, LocalMultiMapStatsImpl>();
    Map<String, LocalReplicatedMapStatsImpl> replicatedMapStats = new HashMap<String, LocalReplicatedMapStatsImpl>();
    Map<String, LocalQueueStatsImpl> queueStats = new HashMap<String, LocalQueueStatsImpl>();
    Map<String, LocalTopicStatsImpl> topicStats = new HashMap<String, LocalTopicStatsImpl>();
    Map<String, LocalExecutorStatsImpl> executorStats = new HashMap<String, LocalExecutorStatsImpl>();
    List<Integer> partitions = new ArrayList<Integer>(271);

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
        out.writeInt(replicatedMapStats.size());
        for (Map.Entry<String, LocalReplicatedMapStatsImpl> entry : replicatedMapStats.entrySet()) {
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
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        address.readData(in);
        DataSerializable impl;
        String name;
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            (impl = new LocalMapStatsImpl()).readData(in);
            mapStats.put(name, (LocalMapStatsImpl) impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            (impl = new LocalMultiMapStatsImpl()).readData(in);
            multiMapStats.put(name, (LocalMultiMapStatsImpl) impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            (impl = new LocalReplicatedMapStatsImpl()).readData(in);
            replicatedMapStats.put(name, (LocalReplicatedMapStatsImpl) impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            (impl = new LocalQueueStatsImpl()).readData(in);
            queueStats.put(name, (LocalQueueStatsImpl) impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            (impl = new LocalTopicStatsImpl()).readData(in);
            topicStats.put(name, (LocalTopicStatsImpl) impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            (impl = new LocalExecutorStatsImpl()).readData(in);
            executorStats.put(name, (LocalExecutorStatsImpl) impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            runtimeProps.put(name, in.readLong());
        }
        for (int i = in.readInt(); i > 0; i--) {
            partitions.add(in.readInt());
        }
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MemberStateImpl that = (MemberStateImpl) o;

        if (address != null ? !address.equals(that.address) : that.address != null) return false;
        if (executorStats != null ? !executorStats.equals(that.executorStats) : that.executorStats != null)
            return false;
        if (mapStats != null ? !mapStats.equals(that.mapStats) : that.mapStats != null) return false;
        if (multiMapStats != null ? !multiMapStats.equals(that.multiMapStats) : that.multiMapStats != null)
            return false;
        if (partitions != null ? !partitions.equals(that.partitions) : that.partitions != null) return false;
        if (queueStats != null ? !queueStats.equals(that.queueStats) : that.queueStats != null) return false;
        if (replicatedMapStats != null ? !replicatedMapStats.equals(that.replicatedMapStats) : that.replicatedMapStats != null)
            return false;
        if (runtimeProps != null ? !runtimeProps.equals(that.runtimeProps) : that.runtimeProps != null) return false;
        if (topicStats != null ? !topicStats.equals(that.topicStats) : that.topicStats != null) return false;

        return true;
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
    public LocalReplicatedMapStats getLocalReplicatedMapStats(String mapName) {
        return replicatedMapStats.get(mapName);
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

    public void putLocalReplicatedMapStats(String name, LocalReplicatedMapStatsImpl localReplicatedMapStats) {
        replicatedMapStats.put(name, localReplicatedMapStats);
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

    @Override
    public String toString() {
        return "MemberStateImpl{" +
                "address=" + address +
                ", runtimeProps=" + runtimeProps +
                ", mapStats=" + mapStats +
                ", multiMapStats=" + multiMapStats +
                ", replicatedMapStats=" + replicatedMapStats +
                ", queueStats=" + queueStats +
                ", topicStats=" + topicStats +
                ", executorStats=" + executorStats +
                ", partitions=" + partitions +
                '}';
    }
}
