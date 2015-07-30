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

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class EntryListenerOnReconnectTest extends AbstractListenersOnReconnectTest {

    private IMap iMap;

    @Override
    protected String addListener(final AtomicInteger eventCount) {
        iMap = client.getMap(randomString());
        final EntryAdapter<Object, Object> listener = new EntryAdapter<Object, Object>() {
            public void onEntryEvent(EntryEvent<Object, Object> event) {
                eventCount.incrementAndGet();
            }
        };
        return iMap.addEntryListener(listener, true);
    }

    @Override
    public void produceEvent() {
        iMap.put(randomString(), randomString());
    }

    @Override
    public boolean removeListener(String registrationId) {
        return iMap.removeEntryListener(registrationId);
    }
}
