/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.impl.DefaultRecord;
import com.hazelcast.impl.Record;
import com.hazelcast.impl.partition.PartitionInfo;
import com.hazelcast.nio.Data;

import java.util.HashMap;
import java.util.Map;

public class PartitionContainer {
    private final Config config;
    private final MapService mapService;
    final PartitionInfo partitionInfo;
    final Map<String, MapPartition> maps = new HashMap<String, MapPartition>(100);
    final Map<String, TransactionLog> transactions = new HashMap<String, TransactionLog>(100);

    public PartitionContainer(Config config, MapService mapService, PartitionInfo partitionInfo) {
        this.config = config;
        this.mapService = mapService;
        this.partitionInfo = partitionInfo;
    }

    MapConfig getMapConfig(String name) {
        return config.findMatchingMapConfig(name.substring(2));
    }

    public MapPartition getMapPartition(String name) {
        MapPartition mapPartition = maps.get(name);
        if (mapPartition == null) {
            mapPartition = new MapPartition(name, PartitionContainer.this);
            maps.put(name, mapPartition);
        }
        return mapPartition;
    }

    public TransactionLog getTransactionLog(String txnId) {
        return transactions.get(txnId);
    }

    public void addTransactionLogItem(String txnId, TransactionLogItem logItem) {
        TransactionLog log = transactions.get(txnId);
        if (log == null) {
            log = new TransactionLog(txnId);
            transactions.put(txnId, log);
        }
        log.addLogItem(logItem);
    }

    public void putTransactionLog(String txnId, TransactionLog txnLog) {
        transactions.put(txnId, txnLog);
    }

    void rollback(String txnId) {
        transactions.remove(txnId);
    }

    void commit(String txnId) {
        TransactionLog txnLog = transactions.get(txnId);
        if (txnLog == null) return;
        for (TransactionLogItem txnLogItem : txnLog.changes.values()) {
            System.out.println(mapService.getNodeService().getThisAddress() + " pc.commit " + txnLogItem);
            MapPartition mapPartition = getMapPartition(txnLogItem.getName());
            Data key = txnLogItem.getKey();
            if (txnLogItem.isRemoved()) {
                mapPartition.records.remove(key);
            } else {
                Record record = mapPartition.records.get(key);
                if (record == null) {
                    record = new DefaultRecord(null, mapPartition.partitionInfo.getPartitionId(), key, txnLogItem.getValue(), -1, -1, mapService.nextId());
                    mapPartition.records.put(key, record);
                } else {
                    record.setValueData(txnLogItem.getValue());
                }
                record.setActive();
                record.setDirty(true);
            }
        }
    }
}
