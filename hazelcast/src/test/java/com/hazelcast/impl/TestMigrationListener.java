/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class TestMigrationListener implements MigrationListener {
    final CountDownLatch latchStarted;
    final CountDownLatch latchCompleted;

    public TestMigrationListener(int expectedStarts, int expectedCompletions) {
        latchStarted = new CountDownLatch(expectedStarts);
        latchCompleted = new CountDownLatch(expectedCompletions);
    }

    public void migrationStarted(MigrationEvent migrationEvent) {
        latchStarted.countDown();
    }

    public void migrationCompleted(MigrationEvent migrationEvent) {
        latchCompleted.countDown();
    }

    public boolean await(int seconds) throws Exception {
        if (!latchStarted.await(seconds, TimeUnit.SECONDS)) {
            return false;
        }
        if (!latchCompleted.await(seconds, TimeUnit.SECONDS)) {
            return false;
        }
        return true;
    }
}
