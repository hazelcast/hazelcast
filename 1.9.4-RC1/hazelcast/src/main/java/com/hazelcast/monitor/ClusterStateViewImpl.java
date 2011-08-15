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

package com.hazelcast.monitor;

import com.hazelcast.core.Member;
import com.hazelcast.core.Prefix;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterStateViewImpl implements ClusterStateView, DataSerializable {

    Map<Member, int[]> memberPartitions = new ConcurrentHashMap<Member, int[]>();
    Set<String> instanceNames = new HashSet<String>();

    public ClusterStateViewImpl(Set<String> instanceNames) {
        this.instanceNames = instanceNames;
    }

    public ClusterStateViewImpl() {
    }

    public void setPartition(Member member, int[] partitions) {
        memberPartitions.put(member, partitions);
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeInt(memberPartitions.size());
        Set<Map.Entry<Member, int[]>> memberStatEntries = memberPartitions.entrySet();
        for (Map.Entry<Member, int[]> memberStatEntry : memberStatEntries) {
            memberStatEntry.getKey().writeData(out);
            int[] partitions = memberStatEntry.getValue();
            out.writeInt(partitions.length);
            for (int partition : partitions) {
                out.writeInt(partition);
            }
        }
        int nameCount = instanceNames.size();
        out.writeInt(nameCount);
        for (String name : instanceNames) {
            out.writeUTF(name);
        }
    }

    public void readData(DataInput in) throws IOException {
        int memberStatsCount = in.readInt();
        for (int i = 0; i < memberStatsCount; i++) {
            Member member = new MemberImpl();
            member.readData(in);
            int partitionCount = in.readInt();
            int[] partitions = new int[partitionCount];
            for (int a = 0; a < partitionCount; a++) {
                partitions[a] = in.readInt();
            }
            memberPartitions.put(member, partitions);
        }
        int nameCount = in.readInt();
        for (int i = 0; i < nameCount; i++) {
            instanceNames.add(in.readUTF());
        }
    }

    private Set<String> getInstances(String prefix) {
        Set<String> names = new HashSet<String>();
        for (String name : instanceNames) {
            if (name.startsWith(prefix)) {
                names.add(name.substring(prefix.length()));
            }
        }
        return names;
    }

    public Set<String> getMaps() {
        return getInstances(Prefix.MAP);
    }

    public Set<String> getMultiMaps() {
        return getInstances(Prefix.MULTIMAP);
    }

    public Set<String> getQueues() {
        return getInstances(Prefix.QUEUE);
    }

    public Set<String> getSets() {
        return getInstances(Prefix.SET);
    }

    public Set<String> getLists() {
        return getInstances(Prefix.AS_LIST);
    }

    public Set<String> getTopics() {
        return getInstances(Prefix.TOPIC);
    }

    public Set<String> getAtomicNumbers() {
    	return getInstances(Prefix.ATOMIC_NUMBER);
    }

    public Set<String> getCountDownLatches() {
        return getInstances(Prefix.COUNT_DOWN_LATCH);
    }

    public Set<String> getSemaphores() {
        return getInstances(Prefix.SEMAPHORE);
    }

    public Set<String> getIdGenerators() {
        return getInstances(Prefix.IDGEN);
    }

    public Set<Member> getMembers() {
        return memberPartitions.keySet();
    }

    public int[] getPartitions(Member member) {
        return memberPartitions.get(member);
    }

    @Override
    public String toString() {
        return "ClusterStateViewImpl{" +
                "instanceNames=" + instanceNames +
                ", memberPartitions=" + memberPartitions +
                '}';
    }
}
