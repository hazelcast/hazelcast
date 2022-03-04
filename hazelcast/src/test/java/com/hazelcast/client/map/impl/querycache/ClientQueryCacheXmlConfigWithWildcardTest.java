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

package com.hazelcast.client.map.impl.querycache;

import com.hazelcast.client.config.ClientClasspathXmlConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.querycache.QueryCacheXmlConfigWildcardTest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientQueryCacheXmlConfigWithWildcardTest extends QueryCacheXmlConfigWildcardTest {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @Before
    public void setUp() throws Exception {
        factory.newHazelcastInstance();
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Override
    protected HazelcastInstance createInstance() {
        ClientClasspathXmlConfig clientConfig = new ClientClasspathXmlConfig(
                "hazelcast-client-querycache-xml-config-wildcard-test.xml");
        return factory.newHazelcastClient(clientConfig);
    }
}
