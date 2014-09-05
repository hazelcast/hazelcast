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

package com.hazelcast.hibernate;

import com.hazelcast.client.HazelcastClientProxy;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.entity.DummyEntity;
import com.hazelcast.hibernate.instance.HazelcastAccessor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.SlowTest;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Environment;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Date;
import java.util.Properties;

import static org.junit.Assert.*;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class CustomPropertiesTest extends HibernateTestSupport {

    @Test
    public void test() {
        Properties props = getDefaultProperties();
        props.put(CacheEnvironment.SHUTDOWN_ON_STOP, "false");
        SessionFactory sf = createSessionFactory(props);
        HazelcastInstance hz = HazelcastAccessor.getHazelcastInstance(sf);
        assertEquals(1, hz.getCluster().getMembers().size());
        MapConfig cfg = hz.getConfig().getMapConfig("com.hazelcast.hibernate.entity.*");
        assertNotNull(cfg);
        assertEquals(30, cfg.getTimeToLiveSeconds());
        assertEquals(50, cfg.getMaxSizeConfig().getSize());
        sf.close();
        assertTrue(hz.getLifecycleService().isRunning());
        hz.shutdown();
    }

    @Test
    public void testNativeClient() throws Exception {
        HazelcastInstance main = Hazelcast.newHazelcastInstance(new ClasspathXmlConfig("hazelcast-custom.xml"));
        Properties props = getDefaultProperties();
        props.remove(CacheEnvironment.CONFIG_FILE_PATH_LEGACY);
        props.setProperty(Environment.CACHE_REGION_FACTORY, HazelcastCacheRegionFactory.class.getName());
        props.setProperty(CacheEnvironment.USE_NATIVE_CLIENT, "true");
        props.setProperty(CacheEnvironment.NATIVE_CLIENT_GROUP, "dev-custom");
        props.setProperty(CacheEnvironment.NATIVE_CLIENT_PASSWORD, "dev-pass");
        props.setProperty(CacheEnvironment.NATIVE_CLIENT_ADDRESS, "localhost");
        SessionFactory sf = createSessionFactory(props);
        HazelcastInstance hz = HazelcastAccessor.getHazelcastInstance(sf);
        assertTrue(hz instanceof HazelcastClientProxy);
        assertEquals(1, main.getCluster().getMembers().size());
        HazelcastClientProxy client = (HazelcastClientProxy) hz;
        ClientConfig clientConfig = client.getClientConfig();
        assertEquals("dev-custom", clientConfig.getGroupConfig().getName());
        assertEquals("dev-pass", clientConfig.getGroupConfig().getPassword());
        assertTrue(clientConfig.getNetworkConfig().isSmartRouting());
        assertTrue(clientConfig.getNetworkConfig().isRedoOperation());
        Hazelcast.newHazelcastInstance(new ClasspathXmlConfig("hazelcast-custom.xml"));
        assertEquals(2, hz.getCluster().getMembers().size());
        main.shutdown();
        Thread.sleep(1000 * 3); // let client to reconnect
        assertEquals(1, hz.getCluster().getMembers().size());

        Session session = sf.openSession();
        Transaction tx = session.beginTransaction();
        session.save(new DummyEntity(1L, "dummy", 0, new Date()));
        tx.commit();
        session.close();

        sf.close();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testNamedInstance() {
        Config config = new Config();
        config.setInstanceName("hibernate");
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        Properties props = getDefaultProperties();
        props.setProperty(Environment.CACHE_REGION_FACTORY, HazelcastCacheRegionFactory.class.getName());
        props.put(CacheEnvironment.HAZELCAST_INSTANCE_NAME, "hibernate");
        props.put(CacheEnvironment.SHUTDOWN_ON_STOP, "false");
        final SessionFactory sf = createSessionFactory(props);
        assertTrue(hz == HazelcastAccessor.getHazelcastInstance(sf));
        sf.close();
        assertTrue(hz.getLifecycleService().isRunning());
        hz.shutdown();
    }

    private Properties getDefaultProperties() {
        Properties props = new Properties();
        props.setProperty(Environment.CACHE_REGION_FACTORY, HazelcastCacheRegionFactory.class.getName());
        props.setProperty(CacheEnvironment.CONFIG_FILE_PATH_LEGACY, "hazelcast-custom.xml");
        return props;
    }
}
