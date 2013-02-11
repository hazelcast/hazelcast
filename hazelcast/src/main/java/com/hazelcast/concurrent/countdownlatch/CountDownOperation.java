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

package com.hazelcast.concurrent.countdownlatch;

import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.WaitNotifyKey;

/**
 * @mdogan 1/10/13
 */
public class CountDownOperation extends BackupAwareCountDownLatchOperation implements Notifier {

    private transient boolean shouldNotify;

    public CountDownOperation() {
    }

    public CountDownOperation(String name) {
        super(name);
    }

    public void run() throws Exception {
        CountDownLatchService service = getService();
        service.countDown(name);
        shouldNotify = service.getCount(name) == 0;
    }

    public boolean shouldBackup() {
        return true;
    }

    public boolean shouldNotify() {
        return shouldNotify;
    }

    public WaitNotifyKey getNotifiedKey() {
        return waitNotifyKey();
    }
}
