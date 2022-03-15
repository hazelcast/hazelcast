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

package com.hazelcast.client.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapLiteMemberTest;
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
public class ClientMapQueryLiteMemberTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory;

    private IMap<Integer, Object> map;

    @Before
    public void setUp() {
        factory = new TestHazelcastFactory();
        factory.newHazelcastInstance(getConfig());
        factory.newHazelcastInstance(getConfig().setLiteMember(true));
        HazelcastInstance client = factory.newHazelcastClient();
        map = client.getMap(randomMapName());
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testMapValuesQuery() {
        MapLiteMemberTest.testMapValuesQuery(map);
    }

    @Test
    public void testMapKeysQuery() {
        MapLiteMemberTest.testMapKeysQuery(map);
    }
}
