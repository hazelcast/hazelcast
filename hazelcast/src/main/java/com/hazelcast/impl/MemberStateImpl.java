/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.monitor.*;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
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
    Map<String, LocalAtomicNumberStatsImpl> atomicNumberStats = new HashMap<String, LocalAtomicNumberStatsImpl>();
    Map<String, LocalCountDownLatchStatsImpl> countDownLatchStats = new HashMap<String, LocalCountDownLatchStatsImpl>();
    Map<String, LocalMapStatsImpl> mapStats = new HashMap<String, LocalMapStatsImpl>();
    Map<String, LocalQueueStatsImpl> queueStats = new HashMap<String, LocalQueueStatsImpl>();
    Map<String, LocalSemaphoreStatsImpl> semaphoreStats = new HashMap<String, LocalSemaphoreStatsImpl>();
    Map<String, LocalTopicStatsImpl> topicStats = new HashMap<String, LocalTopicStatsImpl>();
    List<Integer> lsPartitions = new ArrayList<Integer>(271);

    public void writeData(DataOutput out) throws IOException {
        address.writeData(out);
        memberHealthStats.writeData(out);
        out.writeInt(atomicNumberStats.size());
        for (Map.Entry<String, LocalAtomicNumberStatsImpl> entry : atomicNumberStats.entrySet()) {
            out.writeUTF(entry.getKey());
            entry.getValue().writeData(out);
        }
        out.writeInt(countDownLatchStats.size());
        for (Map.Entry<String, LocalCountDownLatchStatsImpl> entry : countDownLatchStats.entrySet()) {
            out.writeUTF(entry.getKey());
            entry.getValue().writeData(out);
        }
        out.writeInt(mapStats.size());
        for (Map.Entry<String, LocalMapStatsImpl> entry : mapStats.entrySet()) {
            out.writeUTF(entry.getKey());
            entry.getValue().writeData(out);
        }
        out.writeInt(queueStats.size());
        for (Map.Entry<String, LocalQueueStatsImpl> entry : queueStats.entrySet()) {
            out.writeUTF(entry.getKey());
            entry.getValue().writeData(out);
        }
        out.writeInt(semaphoreStats.size());
        for (Map.Entry<String, LocalSemaphoreStatsImpl> entry : semaphoreStats.entrySet()) {
            out.writeUTF(entry.getKey());
            entry.getValue().writeData(out);
        }
        out.writeInt(topicStats.size());
        for (Map.Entry<String, LocalTopicStatsImpl> entry : topicStats.entrySet()) {
            out.writeUTF(entry.getKey());
            entry.getValue().writeData(out);
        }
        out.writeInt(lsPartitions.size());
        for (Integer lsPartition : lsPartitions) {
            out.writeInt(lsPartition);
        }
    }

    public void readData(DataInput in) throws IOException {
        address.readData(in);
        memberHealthStats.readData(in);
        DataSerializable impl;
        String name;
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            (impl = new LocalAtomicNumberStatsImpl()).readData(in);
            atomicNumberStats.put(name, (LocalAtomicNumberStatsImpl) impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            (impl = new LocalCountDownLatchStatsImpl()).readData(in);
            countDownLatchStats.put(name, (LocalCountDownLatchStatsImpl) impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            (impl = new LocalMapStatsImpl()).readData(in);
            mapStats.put(name, (LocalMapStatsImpl) impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            (impl = new LocalQueueStatsImpl()).readData(in);
            queueStats.put(name, (LocalQueueStatsImpl) impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            (impl = new LocalSemaphoreStatsImpl()).readData(in);
            semaphoreStats.put(name, (LocalSemaphoreStatsImpl) impl);
        }
        for (int i = in.readInt(); i > 0; i--) {
            name = in.readUTF();
            (impl = new LocalTopicStatsImpl()).readData(in);
            topicStats.put(name, (LocalTopicStatsImpl) impl);
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

    public MemberHealthStatsImpl getMemberHealthStats() {
        return memberHealthStats;
    }

    public LocalAtomicNumberStats getLocalAtomicNumberStats(String atomicLongName) {
        return atomicNumberStats.get(atomicLongName);
    }

    public LocalCountDownLatchStats getLocalCountDownLatchStats(String countDownLatchName) {
        return countDownLatchStats.get(countDownLatchName);
    }

    public LocalMapStats getLocalMapStats(String mapName) {
        return mapStats.get(mapName);
    }

    public LocalQueueStats getLocalQueueStats(String queueName) {
        return queueStats.get(queueName);
    }

    public LocalSemaphoreStats getLocalSemaphoreStats(String semaphoreName) {
        return semaphoreStats.get(semaphoreName);
    }

    public LocalTopicStats getLocalTopicStats(String topicName) {
        return topicStats.get(topicName);
    }

    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }

    public void putLocalAtomicNumberStats(String name, LocalAtomicNumberStatsImpl localAtomicLongStats) {
        atomicNumberStats.put(name, localAtomicLongStats);
    }

    public void putLocalCountDownLatchStats(String name, LocalCountDownLatchStatsImpl localCountDownLatchStats) {
        countDownLatchStats.put(name, localCountDownLatchStats);
    }

    public void putLocalMapStats(String name, LocalMapStatsImpl localMapStats) {
        mapStats.put(name, localMapStats);
    }

    public void putLocalQueueStats(String name, LocalQueueStatsImpl localQueueStats) {
        queueStats.put(name, localQueueStats);
    }

    public void putLocalSemaphoreStats(String name, LocalSemaphoreStatsImpl localSemaphoreStats) {
        semaphoreStats.put(name, localSemaphoreStats);
    }

    public void putLocalTopicStats(String name, LocalTopicStatsImpl localTopicStats) {
        topicStats.put(name, localTopicStats);
    }

    @Override
    public String toString() {
        return "MemberStateImpl [" + address + "] " +
                "\n{ " +
                "\n\t" + memberHealthStats +
                "\n\tmapStats=" + mapStats +
                "\n\tqueueStats=" + queueStats +
                "\n\tpartitions=" + lsPartitions +
                "\n}";
    }
}
