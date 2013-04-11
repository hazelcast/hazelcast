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
    /**
     *
     */
    private static final long serialVersionUID = -1817978625085375340L;
    Address address = new Address();
    MemberHealthStatsImpl memberHealthStats = new MemberHealthStatsImpl();
    Map<String, Long> runtimeProps = new HashMap<String, Long>();
    Map<String, LocalMapStatsImpl> mapStats = new HashMap<String, LocalMapStatsImpl>();
    Map<String, LocalMultiMapStatsImpl> multiMapStats = new HashMap<String, LocalMultiMapStatsImpl>();
    Map<String, LocalQueueStatsImpl> queueStats = new HashMap<String, LocalQueueStatsImpl>();
    Map<String, LocalTopicStatsImpl> topicStats = new HashMap<String, LocalTopicStatsImpl>();
    Map<String, LocalExecutorStatsImpl> executorStats = new HashMap<String, LocalExecutorStatsImpl>();
    //    Map<String, LocalAtomicLongStatsImpl> atomicNumberStats = new HashMap<String, LocalAtomicLongStatsImpl>();
//    Map<String, LocalCountDownLatchStatsImpl> countDownLatchStats = new HashMap<String, LocalCountDownLatchStatsImpl>();
//    Map<String, LocalSemaphoreStatsImpl> semaphoreStats = new HashMap<String, LocalSemaphoreStatsImpl>();
    List<Integer> lsPartitions = new ArrayList<Integer>(271); //TODO @msk where to get this num from?

    public void writeData(ObjectDataOutput out) throws IOException {
        address.writeData(out);
        memberHealthStats.writeData(out);
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

//        out.writeInt(atomicNumberStats.size());
//        for (Map.Entry<String, LocalAtomicLongStatsImpl> entry : atomicNumberStats.entrySet()) {
//            out.writeUTF(entry.getKey());
//            entry.getValue().writeData(out);
//        }
//        out.writeInt(countDownLatchStats.size());
//        for (Map.Entry<String, LocalCountDownLatchStatsImpl> entry : countDownLatchStats.entrySet()) {
//            out.writeUTF(entry.getKey());
//            entry.getValue().writeData(out);
//        }
//        out.writeInt(semaphoreStats.size());
//        for (Map.Entry<String, LocalSemaphoreStatsImpl> entry : semaphoreStats.entrySet()) {
//            out.writeUTF(entry.getKey());
//            entry.getValue().writeData(out);
//        }
        out.writeInt(runtimeProps.size());
        for (Map.Entry<String, Long> entry : runtimeProps.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeLong(entry.getValue());
        }
        out.writeInt(lsPartitions.size());
        for (Integer lsPartition : lsPartitions) {
            out.writeInt(lsPartition);
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        address.readData(in);
        memberHealthStats.readData(in);
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
//        for (int i = in.readInt(); i > 0; i--) {
//            name = in.readUTF();
//            (impl = new LocalAtomicLongStatsImpl()).readData(in);
//            atomicNumberStats.put(name, (LocalAtomicLongStatsImpl) impl);
//        }
//        for (int i = in.readInt(); i > 0; i--) {
//            name = in.readUTF();
//            (impl = new LocalCountDownLatchStatsImpl()).readData(in);
//            countDownLatchStats.put(name, (LocalCountDownLatchStatsImpl) impl);
//        }
//        for (int i = in.readInt(); i > 0; i--) {
//            name = in.readUTF();
//            (impl = new LocalSemaphoreStatsImpl()).readData(in);
//            semaphoreStats.put(name, (LocalSemaphoreStatsImpl) impl);
//        }
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            runtimeProps.put(name, in.readLong());
        }
        for (int i = in.readInt(); i > 0; i--) {
            lsPartitions.add(in.readInt());
        }
    }

    public void clearPartitions() {
        lsPartitions.clear();
    }

    public void addPartition(int partitionId) {
        lsPartitions.add(partitionId);
    }

    public List<Integer> getPartitions() {
        return lsPartitions;
    }

    @Override
    public int hashCode() {
        int result = address != null ? address.hashCode() : 0;
        result = 31 * result + (memberHealthStats != null ? memberHealthStats.hashCode() : 0);
        result = 31 * result + (mapStats != null ? mapStats.hashCode() : 0);
        result = 31 * result + (multiMapStats != null ? multiMapStats.hashCode() : 0);
        result = 31 * result + (queueStats != null ? queueStats.hashCode() : 0);
        result = 31 * result + (topicStats != null ? topicStats.hashCode() : 0);
        result = 31 * result + (executorStats != null ? executorStats.hashCode() : 0);
//        result = 31 * result + (atomicNumberStats != null ? atomicNumberStats.hashCode() : 0);
//        result = 31 * result + (countDownLatchStats != null ? countDownLatchStats.hashCode() : 0);
//        result = 31 * result + (semaphoreStats != null ? semaphoreStats.hashCode() : 0);
        result = 31 * result + (lsPartitions != null ? lsPartitions.hashCode() : 0);
        return result;
    }

    public MemberHealthStatsImpl getMemberHealthStats() {
        return memberHealthStats;
    }

    public Map<String, Long> getRuntimeProps() {
        return runtimeProps;
    }

    public void setRuntimeProps(Map<String, Long> runtimeProps) {
        this.runtimeProps = runtimeProps;
    }

//    public LocalAtomicLongStats getLocalAtomicNumberStats(String atomicLongName) {
//        return atomicNumberStats.get(atomicLongName);
//    }
//
//    public LocalCountDownLatchStats getLocalCountDownLatchStats(String countDownLatchName) {
//        return countDownLatchStats.get(countDownLatchName);
//    }

    public LocalMapStats getLocalMapStats(String mapName) {
        return mapStats.get(mapName);
    }

    public LocalMultiMapStats getLocalMultiMapStats(String mapName) {
        return multiMapStats.get(mapName);
    }

//    public LocalExecutorStats getInternalExecutorStats(String name) {
//        return internalThroughputStats.get(name);
//    }
//
//    public LocalExecutorStats getExternalExecutorStats(String name) {
//        return throughputStats.get(name);
//    }

    public LocalQueueStats getLocalQueueStats(String queueName) {
        return queueStats.get(queueName);
    }

//    public LocalSemaphoreStats getLocalSemaphoreStats(String semaphoreName) {
//        return semaphoreStats.get(semaphoreName);
//    }

    public LocalTopicStats getLocalTopicStats(String topicName) {
        return topicStats.get(topicName);
    }

    public LocalExecutorStats getLocalExecutorStats(String executorName) {
        return executorStats.get(executorName);
    }

    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }

//    public void putLocalAtomicNumberStats(String name, LocalAtomicLongStatsImpl localAtomicLongStats) {
//        atomicNumberStats.put(name, localAtomicLongStats);
//    }
//
//    public void putLocalCountDownLatchStats(String name, LocalCountDownLatchStatsImpl localCountDownLatchStats) {
//        countDownLatchStats.put(name, localCountDownLatchStats);
//    }

    public void putLocalMapStats(String name, LocalMapStatsImpl localMapStats) {
        mapStats.put(name, localMapStats);
    }

    public void putLocalMultiMapStats(String name, LocalMultiMapStatsImpl localMultiMapStats) {
        multiMapStats.put(name, localMultiMapStats);
    }

    public void putLocalQueueStats(String name, LocalQueueStatsImpl localQueueStats) {
        queueStats.put(name, localQueueStats);
    }

//    public void putLocalSemaphoreStats(String name, LocalSemaphoreStatsImpl localSemaphoreStats) {
//        semaphoreStats.put(name, localSemaphoreStats);
//    }

    public void putLocalTopicStats(String name, LocalTopicStatsImpl localTopicStats) {
        topicStats.put(name, localTopicStats);
    }

    public void putLocalExecutorStats(String name, LocalExecutorStatsImpl localExecutorStats) {
        executorStats.put(name, localExecutorStats);
    }

    public String toString() {
        return "MemberStateImpl{" +
                "address=" + address +
                ", memberHealthStats=" + memberHealthStats +
                ", runtimeProps=" + runtimeProps +
                ", mapStats=" + mapStats +
                ", multiMapStats=" + multiMapStats +
                ", queueStats=" + queueStats +
                ", topicStats=" + topicStats +
                ", executorStats=" + executorStats +
                ", lsPartitions=" + lsPartitions +
                '}';
    }
}
