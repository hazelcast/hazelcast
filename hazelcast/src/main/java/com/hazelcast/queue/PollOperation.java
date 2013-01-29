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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;

/**
 * @ali 12/6/12
 */
public class PollOperation extends QueueBackupAwareOperation implements WaitSupport, Notifier, IdentifiedDataSerializable {

    public PollOperation() {
    }

    public PollOperation(String name, long timeoutMillis) {
        super(name, timeoutMillis);
    }

    public void run() {
        response = getContainer().poll();
    }

    public void afterRun() throws Exception {
        if (response != null) {
            publishEvent(ItemEventType.REMOVED, (Data) response);
        }
    }

    public boolean shouldBackup() {
        return response != null;
    }

    public Operation getBackupOperation() {
        return new PollBackupOperation(name);
    }

    public boolean shouldNotify() {
        return response != null;
    }

    public WaitNotifyKey getNotifiedKey() {
        return new QueueWaitNotifyKey(getName(), "offer");
    }

    public WaitNotifyKey getWaitKey() {
        return new QueueWaitNotifyKey(getName(), "poll");
    }

    public boolean shouldWait() {
        return getWaitTimeoutMillis() != 0 && getContainer().size() == 0;
    }

    public void onWaitExpire() {
        getResponseHandler().sendResponse(null);
    }

    public int getId() {
        return DataSerializerQueueHook.POLL;
    }
}
