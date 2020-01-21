/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics.jmx;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.HazelcastTestSupport.assertFalseEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static java.util.concurrent.TimeUnit.SECONDS;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MemberJmxMetricsTest {
    private static final String DOMAIN_PREFIX = "com.hazelcast";

    private TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private JmxPublisherTestHelper helper;

    @Before
    public void setUp() throws Exception {
        helper = new JmxPublisherTestHelper(DOMAIN_PREFIX);
        helper.assertNoMBeans();
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
        helper.assertNoMBeans();
    }

    @Test
    public void testNoMBeanLeak() {
        System.setProperty(ClusterProperty.ENABLE_JMX.getName(), "true");
        helper.assertNoMBeans();

        Config config = smallInstanceConfig();
        config.getMetricsConfig()
              .setCollectionFrequencySeconds(1);

        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        String expectedObjectName = DOMAIN_PREFIX + ":type=Metrics,instance=" + member.getName() + ",prefix=os";
        assertTrueEventually(() -> helper.assertMBeanContains(expectedObjectName));

        member.shutdown();
        helper.assertNoMBeans();
    }

    @Test
    public void testMetricsExposedOverJmxIfSysPropIsSet() {
        System.setProperty(ClusterProperty.ENABLE_JMX.getName(), "true");
        helper.assertNoMBeans();

        Config config = smallInstanceConfig();
        config.getMetricsConfig()
              .setCollectionFrequencySeconds(1);

        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        String expectedObjectName = DOMAIN_PREFIX + ":type=Metrics,instance=" + member.getName() + ",prefix=os";
        assertTrueEventually(() -> helper.assertMBeanContains(expectedObjectName));

        member.shutdown();
        helper.assertNoMBeans();
    }

    @Test
    public void testMetricsNotExposedOverJmxIfSysPropIsNotSet() throws Exception {
        helper.assertNoMBeans();

        Config config = smallInstanceConfig();
        config.getMetricsConfig()
              .setCollectionFrequencySeconds(1);

        // we wait for two metrics collections before assert on JMX exposure
        CountDownLatch collectionLatch = new CountDownLatch(2);
        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        getNodeEngineImpl(member).getMetricsRegistry()
                                 .registerDynamicMetricsProvider((descriptor, context) -> collectionLatch.countDown());

        collectionLatch.await(2, SECONDS);

        helper.assertNoMBeans();
        member.shutdown();
    }

    @Test
    public void testMetricsNotExposedOverJmxIfSysPropIsSetButJmxConfigDisabled() throws Exception {
        System.setProperty(ClusterProperty.ENABLE_JMX.getName(), "true");
        helper.assertNoMBeans();

        Config config = smallInstanceConfig();
        config.getMetricsConfig()
              .setCollectionFrequencySeconds(1)
              .getJmxConfig().setEnabled(false);

        // we wait for two metrics collections before assert on JMX exposure
        CountDownLatch collectionLatch = new CountDownLatch(2);
        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        getNodeEngineImpl(member).getMetricsRegistry()
                                 .registerDynamicMetricsProvider((descriptor, context) -> collectionLatch.countDown());

        collectionLatch.await(5, SECONDS);

        // we can't use helper.assertNoMBeans() here because there are some, but no metrics related is expected
        String expectedObjectName = DOMAIN_PREFIX + ":type=Metrics,instance=" + member.getName() + ",prefix=os";
        assertFalseEventually(() -> helper.assertMBeanContains(expectedObjectName));

        member.shutdown();
        helper.assertNoMBeans();
    }
}
