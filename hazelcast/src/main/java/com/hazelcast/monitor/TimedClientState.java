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

package com.hazelcast.monitor;

import com.hazelcast.cache.impl.CacheStatsImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * date: 13/03/14
 * author: eminn
 */
public class TimedClientState implements DataSerializable {

    private long time;
    private String cluster;
    private String clientAddress;
    private HashMap<String, CacheStatsImpl> statsMap;

    public TimedClientState() {
    }

    public TimedClientState(String clientAddress, String cluster, long time) {
        this.clientAddress = clientAddress;
        this.cluster = cluster;
        this.time = time;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getClientAddress() {
        return clientAddress;
    }

    public void setClientAddress(String clientAddress) {
        this.clientAddress = clientAddress;
    }

    public Map<String, CacheStatsImpl> getStatsMap() {
        return statsMap;
    }

    public void setStatsMap(Map<String, CacheStatsImpl> statsMap) {
        this.statsMap = (HashMap) statsMap;
    }


    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(time);
        out.writeUTF(clientAddress);
        out.writeUTF(cluster);
        int size = statsMap.size();
        out.writeInt(size);
        for (String key : statsMap.keySet()) {
            out.writeUTF(key);
            statsMap.get(key).writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        time = in.readLong();
        clientAddress = in.readUTF();
        cluster = in.readUTF();
        int size = in.readInt();
        statsMap = new HashMap<String, CacheStatsImpl>();
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            CacheStatsImpl stats = new CacheStatsImpl();
            stats.readData(in);
            statsMap.put(key, stats);
        }
    }
}
