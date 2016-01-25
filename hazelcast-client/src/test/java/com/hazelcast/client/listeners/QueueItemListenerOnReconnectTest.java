/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.listeners;

import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueueItemListenerOnReconnectTest extends AbstractListenersOnReconnectTest {

    private IQueue iQueue;

    @Override
    protected String addListener(final AtomicInteger eventCount) {
        iQueue = client.getQueue(randomString());
        ItemListener listener = new ItemListener() {
            @Override
            public void itemAdded(ItemEvent item) {
                eventCount.incrementAndGet();
            }

            @Override
            public void itemRemoved(ItemEvent item) {

            }
        };
        return iQueue.addItemListener(listener, true);
    }

    @Override
    public void produceEvent() {
        iQueue.add(randomString());
    }

    @Override
    public boolean removeListener(String registrationId) {
        return iQueue.removeItemListener(registrationId);
    }
}