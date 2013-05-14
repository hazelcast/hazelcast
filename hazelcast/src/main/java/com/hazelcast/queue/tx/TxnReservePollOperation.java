/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.queue.QueueDataSerializerHook;
import com.hazelcast.queue.QueueOperation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;

/**
 * @ali 3/27/13
 */
public class TxnReservePollOperation extends QueueOperation implements WaitSupport {

    public TxnReservePollOperation() {
    }

    public TxnReservePollOperation(String name, long timeoutMillis) {
        super(name, timeoutMillis);
    }

    public void run() throws Exception {
        response = getOrCreateContainer().txnPollReserve();
    }

    public WaitNotifyKey getWaitKey() {
        return getOrCreateContainer().getPollWaitNotifyKey();
    }

    public boolean shouldWait() {
        return getWaitTimeoutMillis() != 0 && getOrCreateContainer().size() == 0;
    }

    public void onWaitExpire() {
        getResponseHandler().sendResponse(null);
    }

    public int getId() {
        return QueueDataSerializerHook.TXN_RESERVE_POLL;
    }
}
