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
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
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
public class ClientMapInMemoryFormatTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @Before
    public void setUp() throws Exception {
        factory = new TestHazelcastFactory();
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testIMapCreation_throwsException_whenInMemoryFormat_NATIVE() throws Exception {
        Config config = getConfig();
        config.getMapConfig("default").setInMemoryFormat(InMemoryFormat.NATIVE);

        HazelcastInstance member = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        client.getMap("default");
    }


    @Test(expected = InvalidConfigurationException.class)
    public void testNearCacheCreation_throwsException_whenInMemoryFormat_NATIVE() throws Exception {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);

        Config config = getConfig();
        config.getMapConfig("default").setNearCacheConfig(nearCacheConfig);

        HazelcastInstance member = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        client.getMap("default");
    }
}
