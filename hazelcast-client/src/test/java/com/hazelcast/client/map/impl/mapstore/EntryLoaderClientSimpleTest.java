/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.map.impl.mapstore;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.map.impl.mapstore.EntryLoaderSimpleTest;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EntryLoaderClientSimpleTest extends EntryLoaderSimpleTest {

    private TestHazelcastFactory factory;

    @Before
    public void setup() {
        factory = new TestHazelcastFactory(1);
        instances = factory.newInstances(getConfig());
        map = factory.newHazelcastClient().getMap(randomMapName());
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }
}
