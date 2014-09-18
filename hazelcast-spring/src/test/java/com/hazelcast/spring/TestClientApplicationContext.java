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

package com.hazelcast.spring;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.HazelcastClientProxy;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ProxyFactoryConfig;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.MultiMap;
import com.hazelcast.security.Credentials;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"node-client-applicationContext-hazelcast.xml"})
@Category(QuickTest.class)
public class TestClientApplicationContext {

    @Resource(name = "client")
    private HazelcastClientProxy client;

    @Resource(name = "client2")
    private HazelcastClientProxy client2;

    @Resource(name = "client3")
    private HazelcastClientProxy client3;

    @Resource(name = "client4")
    private HazelcastClientProxy client4;

    @Resource(name = "instance")
    private HazelcastInstance instance;

    @Resource(name = "map1")
    private IMap<Object, Object> map1;

    @Resource(name = "map2")
    private IMap<Object, Object> map2;

    @Resource(name = "multiMap")
    private MultiMap multiMap;

    @Resource(name = "queue")
    private IQueue queue;

    @Resource(name = "topic")
    private ITopic topic;

    @Resource(name = "set")
    private ISet set;

    @Resource(name = "list")
    private IList list;

    @Resource(name = "executorService")
    private ExecutorService executorService;

    @Resource(name = "idGenerator")
    private IdGenerator idGenerator;

    @Resource(name = "atomicLong")
    private IAtomicLong atomicLong;

    @Resource(name = "atomicReference")
    private IAtomicReference atomicReference;

    @Resource(name = "countDownLatch")
    private ICountDownLatch countDownLatch;

    @Resource(name = "semaphore")
    private ISemaphore semaphore;

    @Autowired
    private Credentials credentials;

    @BeforeClass
    @AfterClass
    public static void start() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testClient() {
        assertNotNull(client);
        assertNotNull(client2);
        assertNotNull(client3);

        ClientConfig config = client.getClientConfig();
        assertEquals("13", config.getProperty("hazelcast.client.retry.count"));
        assertEquals(3, config.getNetworkConfig().getConnectionAttemptLimit());
        assertEquals(1000, config.getNetworkConfig().getConnectionTimeout());
        assertEquals(3000, config.getNetworkConfig().getConnectionAttemptPeriod());

        ClientConfig config2 = client2.getClientConfig();
        assertEquals(credentials, config2.getSecurityConfig().getCredentials());

        client.getMap("default").put("Q", "q");
        client2.getMap("default").put("X", "x");

        final IMap<Object, Object> map = instance.getMap("default");
        assertEquals("q", map.get("Q"));
        assertEquals("x", map.get("X"));

        ClientConfig config3 = client3.getClientConfig();
        final SerializationConfig serConf = config3.getSerializationConfig();

        assertEquals(ByteOrder.BIG_ENDIAN, serConf.getByteOrder());
        assertEquals(false, serConf.isAllowUnsafe());
        assertEquals(false, serConf.isCheckClassDefErrors());
        assertEquals(false, serConf.isEnableCompression());
        assertEquals(false, serConf.isEnableSharedObject());
        assertEquals(false, serConf.isUseNativeByteOrder());
        assertEquals(10, serConf.getPortableVersion());

        final Map<Integer, String> map1 = serConf.getDataSerializableFactoryClasses();
        assertNotNull(map1);
        assertTrue(map1.containsKey(1));
        assertEquals("com.hazelcast.spring.serialization.DummyDataSerializableFactory", map1.get(1));

        final Map<Integer, String> portableFactoryClasses = serConf.getPortableFactoryClasses();
        assertNotNull(portableFactoryClasses);
        assertTrue(portableFactoryClasses.containsKey(2));
        assertEquals("com.hazelcast.spring.serialization.DummyPortableFactory", portableFactoryClasses.get(2));

        final Collection<SerializerConfig> serializerConfigs = serConf.getSerializerConfigs();

        assertNotNull(serializerConfigs);

        final SerializerConfig serializerConfig = serializerConfigs.iterator().next();
        assertNotNull(serializerConfig);
        assertEquals("com.hazelcast.nio.serialization.CustomSerializationTest$FooXmlSerializer", serializerConfig.getClassName());
        assertEquals("com.hazelcast.nio.serialization.CustomSerializationTest$Foo", serializerConfig.getTypeClassName());

        final List<ProxyFactoryConfig> proxyFactoryConfigs = config3.getProxyFactoryConfigs();
        assertNotNull(proxyFactoryConfigs);
        final ProxyFactoryConfig proxyFactoryConfig = proxyFactoryConfigs.get(0);
        assertNotNull(proxyFactoryConfig);
        assertEquals("com.hazelcast.spring.DummyProxyFactory", proxyFactoryConfig.getClassName());
        assertEquals("MyService", proxyFactoryConfig.getService());

        final LoadBalancer loadBalancer = config3.getLoadBalancer();
        assertNotNull(loadBalancer);

        assertTrue(loadBalancer instanceof RoundRobinLB);

        final NearCacheConfig nearCacheConfig = config3.getNearCacheConfig("default");

        assertNotNull(nearCacheConfig);

        assertEquals(1, nearCacheConfig.getTimeToLiveSeconds());
        assertEquals(70, nearCacheConfig.getMaxIdleSeconds());
        assertEquals("LRU", nearCacheConfig.getEvictionPolicy());
        assertEquals(4000, nearCacheConfig.getMaxSize());
        assertEquals(true, nearCacheConfig.isInvalidateOnChange());
    }

    @Test
    public void testAwsClientConfig() {
        assertNotNull(client4);
        ClientConfig config = client4.getClientConfig();
        final ClientNetworkConfig networkConfig = config.getNetworkConfig();

        final ClientAwsConfig awsConfig = networkConfig.getAwsConfig();
        assertFalse(awsConfig.isEnabled());
        assertTrue(awsConfig.isInsideAws());
        assertEquals("sample-access-key", awsConfig.getAccessKey());
        assertEquals("sample-secret-key", awsConfig.getSecretKey());
        assertEquals("sample-region", awsConfig.getRegion());
        assertEquals("sample-group", awsConfig.getSecurityGroupName());
        assertEquals("sample-tag-key", awsConfig.getTagKey());
        assertEquals("sample-tag-value", awsConfig.getTagValue());
    }

    @Test
    public void testHazelcastInstances() {
        assertNotNull(map1);
        assertNotNull(map2);
        assertNotNull(multiMap);
        assertNotNull(queue);
        assertNotNull(topic);
        assertNotNull(set);
        assertNotNull(list);
        assertNotNull(executorService);
        assertNotNull(idGenerator);
        assertNotNull(atomicLong);
        assertNotNull(atomicReference);
        assertNotNull(countDownLatch);
        assertNotNull(semaphore);
        assertEquals("map1", map1.getName());
        assertEquals("map2", map2.getName());
        assertEquals("multiMap", multiMap.getName());
        assertEquals("queue", queue.getName());
        assertEquals("topic", topic.getName());
        assertEquals("set", set.getName());
        assertEquals("list", list.getName());
        assertEquals("idGenerator", idGenerator.getName());
        assertEquals("atomicLong", atomicLong.getName());
        assertEquals("atomicReference", atomicReference.getName());
        assertEquals("countDownLatch", countDownLatch.getName());
        assertEquals("semaphore", semaphore.getName());
    }
}
