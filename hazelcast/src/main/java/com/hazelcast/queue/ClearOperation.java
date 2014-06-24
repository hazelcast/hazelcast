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

package com.hazelcast.queue;

import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;

import java.util.Map;
/**
 * Clears items stored by Queue.
 */
public class ClearOperation extends QueueBackupAwareOperation implements Notifier {

    private Map<Long, Data> dataMap;

    public ClearOperation() {
    }

    public ClearOperation(String name) {
        super(name);
    }

    @Override
    public void run() {
        dataMap = getOrCreateContainer().clear();
        response = true;
    }

    @Override
    public void afterRun() throws Exception {
        getQueueService().getLocalQueueStatsImpl(name).incrementOtherOperations();
        for (Data data : dataMap.values()) {
            publishEvent(ItemEventType.REMOVED, data);
        }
    }

    @Override
    public Operation getBackupOperation() {
        return new ClearBackupOperation(name, dataMap.keySet());
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(!dataMap.isEmpty());
    }

    @Override
    public boolean shouldNotify() {
        return Boolean.TRUE.equals(!dataMap.isEmpty());
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return getOrCreateContainer().getOfferWaitNotifyKey();
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.CLEAR;
    }
}
