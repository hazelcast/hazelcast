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

package com.hazelcast.queue.tx;

import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.PollBackupOperation;
import com.hazelcast.queue.QueueWaitNotifyKey;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;

/**
 * @ali 12/6/12
 */
public class TxPollOperation extends TransactionalQueueOperation {

    private transient Data response;

    public TxPollOperation() {
    }

    public TxPollOperation(String name, String txnId, long timeoutMillis) {
        super(name, txnId, timeoutMillis);
    }

//    public void run() {
//        response = getContainer().poll();
//    }

    public Object getResponse() {
        return response;
    }

    public void innerAfterRun() throws Exception {
        if (response != null) {
            publishEvent(ItemEventType.REMOVED, response);
        }
    }

    public boolean shouldBackup() {
        return response != null && isCommitted();
    }

    public Operation getBackupOperation() {
        return new PollBackupOperation(name);
    }

    public boolean shouldNotify() {
        return response != null && isCommitted();
    }

    public WaitNotifyKey getNotifiedKey() {
        return new QueueWaitNotifyKey(name, "offer");
    }

    public WaitNotifyKey getWaitKey() {
        return new QueueWaitNotifyKey(name, "poll");
    }

    public boolean shouldWait() {
        return getWaitTimeoutMillis() != 0 && getContainer().size() == 0;
    }

    public void onWaitExpire() {
        getResponseHandler().sendResponse(null);
    }
}
