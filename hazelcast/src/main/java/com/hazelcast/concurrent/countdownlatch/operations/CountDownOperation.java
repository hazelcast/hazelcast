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
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.impl.MutatingOperation;

import static com.hazelcast.concurrent.countdownlatch.CountDownLatchDataSerializerHook.COUNT_DOWN_OPERATION;

public class CountDownOperation extends BackupAwareCountDownLatchOperation implements Notifier, MutatingOperation {

    private boolean shouldNotify;

    public CountDownOperation() {
    }

    public CountDownOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        CountDownLatchService service = getService();
        service.countDown(name);
        int count = service.getCount(name);
        shouldNotify = count == 0;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public boolean shouldNotify() {
        return shouldNotify;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return waitNotifyKey();
    }

    @Override
    public int getId() {
        return COUNT_DOWN_OPERATION;
    }
}
