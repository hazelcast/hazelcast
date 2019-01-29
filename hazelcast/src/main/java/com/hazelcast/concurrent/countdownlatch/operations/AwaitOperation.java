/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.countdownlatch.operations;

import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.WaitNotifyKey;

import static com.hazelcast.concurrent.countdownlatch.CountDownLatchDataSerializerHook.AWAIT_OPERATION;
import static java.lang.Boolean.TRUE;

public class AwaitOperation extends AbstractCountDownLatchOperation implements BlockingOperation, ReadonlyOperation {

    public AwaitOperation() {
    }

    public AwaitOperation(String name, long timeout) {
        super(name);
        setWaitTimeout(timeout);
    }

    @Override
    public void run() throws Exception {
    }

    @Override
    public Object getResponse() {
        return TRUE;
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return waitNotifyKey();
    }

    @Override
    public boolean shouldWait() {
        CountDownLatchService service = getService();
        return service.shouldWait(name);
    }

    @Override
    public void onWaitExpire() {
        sendResponse(false);
    }

    @Override
    public int getId() {
        return AWAIT_OPERATION;
    }
}
