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

package com.hazelcast.client.internal.metrics;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.jmx.JmxPublisherTestHelper;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientJmxMetricsTest extends HazelcastTestSupport {
    private static final String DOMAIN_PREFIX = "com.hazelcast";

    private TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private JmxPublisherTestHelper helper;

    @Before
    public void setUp() throws Exception {
        helper = new JmxPublisherTestHelper(DOMAIN_PREFIX);
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testNoMBeanLeak() {
        helper.clearMBeans();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getMetricsConfig()
                    .setCollectionFrequencySeconds(1);
        clientConfig.getConnectionStrategyConfig()
                    .setAsyncStart(true);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        String expectedObjectName = DOMAIN_PREFIX + ":type=Metrics,instance=" + client.getName() + ",prefix=os";
        assertTrueEventually(() -> helper.assertMBeanContains(expectedObjectName));

        client.shutdown();
        helper.assertNoMBeans();
    }
}
