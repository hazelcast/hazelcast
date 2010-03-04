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

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import org.junit.Ignore;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Ignore 
public class TestEntryListener implements EntryListener {
    final CountDownLatch latchAdded;
    final CountDownLatch latchRemoved;
    final CountDownLatch latchUpdated;
    final CountDownLatch latchEvicted;

    public TestEntryListener(int expectedAdds, int expectedRemoves, int expectedUpdates, int expectedEvicts) {
        latchAdded = new CountDownLatch(expectedAdds);
        latchRemoved = new CountDownLatch(expectedRemoves);
        latchUpdated = new CountDownLatch(expectedUpdates);
        latchEvicted = new CountDownLatch(expectedEvicts);
    }

    public void entryAdded(EntryEvent entryEvent) {
        latchAdded.countDown();
    }

    public void entryRemoved(EntryEvent entryEvent) {
    }

    public void entryUpdated(EntryEvent entryEvent) {
    }

    public void entryEvicted(EntryEvent entryEvent) {
        latchEvicted.countDown();
    }

    public boolean await(int seconds) throws Exception {
        if (!latchAdded.await(seconds, TimeUnit.SECONDS)) {
            return false;
        }
        if (!latchRemoved.await(seconds, TimeUnit.SECONDS)) {
            return false;
        }
        if (!latchUpdated.await(seconds, TimeUnit.SECONDS)) {
            return false;
        }
        if (!latchEvicted.await(seconds, TimeUnit.SECONDS)) {
            return false;
        }
        return true;
    }
}