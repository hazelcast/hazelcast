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

package com.hazelcast.client;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SimpleProtocolTest extends HazelcastTestSupport {

    @After
    @Before
    public void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testPut() {
        Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IMap<Object, Object> map = client.getMap(randomMapName());

        String key = randomString();
        String value = randomString();
        map.put(key, value);
        Object oldValue = map.put(key, randomString());
        assertEquals(value, oldValue);
    }


    @Test
    public void testListener() {
        Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IMap<Object, Object> map = client.getMap(randomMapName());
        final CountDownLatch latch = new CountDownLatch(2);

        final String key = randomString();
        final String value = randomString();
        final String newValue = randomString();

        map.addEntryListener(new EntryAddedListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                if (key.equals(event.getKey()) && value.equals(event.getValue())) {
                    latch.countDown();
                }
            }
        }, true);

        map.addEntryListener(new EntryUpdatedListener() {
            @Override
            public void entryUpdated(EntryEvent event) {
                if (key.equals(event.getKey()) && newValue.equals(event.getValue())) {
                    latch.countDown();
                }
            }
        }, true);


        map.put(key, value);
        map.put(key, newValue);
        assertOpenEventually(latch);

    }


}
