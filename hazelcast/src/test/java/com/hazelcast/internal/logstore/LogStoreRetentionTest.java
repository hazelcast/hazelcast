/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.logstore;

import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class LogStoreRetentionTest extends AbstractLogStoreTest {

    @Test
    public void test() throws InterruptedException {
        LogStoreConfig config = new LogStoreConfig()
                .setTenuringAgeMillis(100)
                .setRetentionMillis(1000);

        IntLogStore logStore = createIntLogStore(config);
        long endNanos = System.currentTimeMillis() + 2000;
        while (endNanos > System.currentTimeMillis()) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            logStore.touch();
            logStore.putInt(10);
        }

        System.out.println(logStore.segmentCount);
        System.out.println(logStore.count());
    }
}
