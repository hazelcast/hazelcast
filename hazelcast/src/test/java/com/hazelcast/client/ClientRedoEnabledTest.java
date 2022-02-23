/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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


import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientRedoEnabledTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance client;

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void init() {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().setRedoOperation(true);
        hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient(config);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testList_get_throwsIndexOutOfBound() {
        IList<Object> list = client.getList("list");
        list.get(0);
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testMap_unlock_throwsIllegalMonitorStateException() {
        IMap<Object, Object> map = client.getMap("map");
        map.unlock("foo");
    }
}
