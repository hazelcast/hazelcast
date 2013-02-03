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

package com.hazelcast.impl;

import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import org.junit.Ignore;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Ignore
class TestMigrationListener implements MigrationListener {
    final CountDownLatch latchStarted;
    final CountDownLatch latchCompleted;

    public TestMigrationListener(int expectedStarts, int expectedCompletions) {
        latchStarted = new CountDownLatch(expectedStarts);
        latchCompleted = new CountDownLatch(expectedCompletions);
    }

    public void migrationStarted(MigrationEvent migrationEvent) {
//        System.out.println(latchStarted.getCount() + " started " + migrationEvent);
        latchStarted.countDown();
    }

    public void migrationCompleted(MigrationEvent migrationEvent) {
//        System.out.println(latchCompleted.getCount() + " completed " + migrationEvent);
        latchCompleted.countDown();
    }

    public void migrationFailed(final MigrationEvent migrationEvent) {

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

    @Override
    public String toString() {
        return "TestMigrationListener{" +
                "latchStarted=" + latchStarted.getCount() +
                ", latchCompleted=" + latchCompleted.getCount() +
                '}';
    }
}
