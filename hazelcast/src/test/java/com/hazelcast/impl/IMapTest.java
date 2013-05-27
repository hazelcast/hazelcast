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

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.nio.Data;
import org.junit.*;
import org.junit.runner.RunWith;
import static com.hazelcast.nio.IOUtil.toData;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class IMapTest extends TestUtil {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }


    @Test
    public void testContainsKeyUpdatesLastAccessTime() throws Exception {
        Config config = new Config();
        FactoryImpl mockFactory = mock(FactoryImpl.class);
        // we mocked the node
        // do not forget to shutdown the connectionManager
        // so that server socket can be released.
        Node node = new Node(mockFactory, config);
        node.serviceThread = Thread.currentThread();
        IMap imap = Hazelcast.getMap("c:myMap");
        Object key = "1";
        Object value = "istanbul";
        Data dKey = toData(key);
        Data dValue = toData(value);
        imap.put(dKey, dValue);
        MapEntry entry = imap.getMapEntry(dKey);
        long firstAccessTime = entry.getLastAccessTime();
        Thread.sleep(10);
        assertTrue(imap.containsKey(dKey));
        entry = imap.getMapEntry(dKey);
        long secondAccessTime = entry.getLastAccessTime();
        assertTrue("entry access time should have been updated on get()", secondAccessTime > firstAccessTime);
        node.connectionManager.shutdown();
    }

    @Test
    public void testGetUpdatesLastAccessTime() throws Exception {
        Config config = new Config();
        FactoryImpl mockFactory = mock(FactoryImpl.class);
        // we mocked the node
        // do not forget to shutdown the connectionManager
        // so that server socket can be released.
        Node node = new Node(mockFactory, config);
        node.serviceThread = Thread.currentThread();
        IMap imap = Hazelcast.getMap("c:myMap");
        Object key = "1";
        Object value = "istanbul";
        Data dKey = toData(key);
        Data dValue = toData(value);
        imap.put(dKey, dValue);
        MapEntry entry = imap.getMapEntry(dKey);
        long firstAccessTime = entry.getLastAccessTime();
        Thread.sleep(10);
        assertTrue(imap.get(dKey).equals(value));
        entry = imap.getMapEntry(dKey);
        long secondAccessTime = entry.getLastAccessTime();
        assertTrue("entry access time should have been updated on get()", secondAccessTime > firstAccessTime);
        node.connectionManager.shutdown();
    }
}
