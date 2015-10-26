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

import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SetItemListenerOnReconnectTest extends AbstractListenersOnReconnectTest {

    private ISet iSet;

    @Override
    protected String addListener(final AtomicInteger eventCount) {
        iSet = client.getSet(randomString());
        ItemListener listener = new ItemListener() {
            @Override
            public void itemAdded(ItemEvent item) {
                eventCount.incrementAndGet();
            }

            @Override
            public void itemRemoved(ItemEvent item) {

            }
        };
        return iSet.addItemListener(listener, true);
    }

    @Override
    public void produceEvent() {
        iSet.add(randomString());
    }

    @Override
    public boolean removeListener(String registrationId) {
        return iSet.removeItemListener(registrationId);
    }
}
