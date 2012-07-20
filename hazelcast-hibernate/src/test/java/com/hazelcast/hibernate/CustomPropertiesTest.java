/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.entity.DummyEntity;
import com.hazelcast.hibernate.instance.HazelcastAccessor;
import com.hazelcast.hibernate.provider.HazelcastCacheProvider;
import org.hibernate.*;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.CacheKey;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.SessionFactoryImplementor;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Properties;

import static org.junit.Assert.*;

@RunWith(TestBlockJUnit4ClassRunner.class)
public class CustomPropertiesTest extends HibernateTestSupport {

    @BeforeClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void start() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void test() {
        Properties props = getDefaultProperties();
        props.put(CacheEnvironment.SHUTDOWN_ON_STOP, "false");
        SessionFactory sf = createSessionFactory(props);
        HazelcastInstance hz = HazelcastAccessor.getHazelcastInstance(sf);
        assertNotSame(Hazelcast.getDefaultInstance(), hz);
        assertEquals(1, hz.getCluster().getMembers().size());
        MapConfig cfg = hz.getConfig().getMapConfig("com.hazelcast.hibernate.entity.*");
        assertNotNull(cfg);
        assertEquals(30, cfg.getTimeToLiveSeconds());
        assertEquals(50, cfg.getMaxSizeConfig().getSize());
        Hazelcast.getDefaultInstance().getLifecycleService().shutdown();
        sf.close();
        assertTrue(hz.getLifecycleService().isRunning());
        hz.getLifecycleService().shutdown();
    }

    @Test
    public void testLiteMember() throws Exception {
        HazelcastInstance main = Hazelcast.newHazelcastInstance(new ClasspathXmlConfig("hazelcast-custom.xml"));
        Properties props = getDefaultProperties();
        props.setProperty(CacheEnvironment.USE_LITE_MEMBER, "true");
        SessionFactory sf = createSessionFactory(props);
        HazelcastInstance hz = HazelcastAccessor.getHazelcastInstance(sf);
        assertTrue(hz.getCluster().getLocalMember().isLiteMember());
        assertEquals(2, main.getCluster().getMembers().size());
        sf.close();
        main.getLifecycleService().shutdown();
    }

    @Test
    public void testNativeClient() throws Exception {
        HazelcastInstance main = Hazelcast.newHazelcastInstance(new ClasspathXmlConfig("hazelcast-custom.xml"));
        Properties props = getDefaultProperties();
        props.setProperty(CacheEnvironment.USE_NATIVE_CLIENT, "true");
        props.setProperty(CacheEnvironment.NATIVE_CLIENT_GROUP, "dev-custom");
        props.setProperty(CacheEnvironment.NATIVE_CLIENT_PASSWORD, "dev-pass");
        props.setProperty(CacheEnvironment.NATIVE_CLIENT_ADDRESS, "localhost");
        SessionFactory sf = createSessionFactory(props);
        HazelcastInstance hz = HazelcastAccessor.getHazelcastInstance(sf);
        assertTrue(hz instanceof HazelcastClient);
        assertEquals(1, main.getCluster().getMembers().size());
        HazelcastClient client = (HazelcastClient) hz;
        ClientConfig clientConfig = client.getClientConfig();
        assertEquals("dev-custom", clientConfig.getGroupConfig().getName());
        assertEquals("dev-pass", clientConfig.getGroupConfig().getPassword());
        Hazelcast.newHazelcastInstance(new ClasspathXmlConfig("hazelcast-custom.xml"));
        assertEquals(2, hz.getCluster().getMembers().size());
        main.getLifecycleService().shutdown();
        Thread.sleep(1000 * 2); // let client to reconnect
        assertEquals(1, hz.getCluster().getMembers().size());
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
        hz.getLifecycleService().shutdown();
    }

    @Test(expected = CacheException.class)
    public void testTimeout() throws InterruptedException {
        Properties props = getDefaultProperties();
        props.setProperty(Environment.CACHE_REGION_FACTORY, HazelcastCacheRegionFactory.class.getName());
        props.put(CacheEnvironment.LOCK_TIMEOUT, "3");
        final SessionFactory sf = createSessionFactory(props);
        assertEquals(3, CacheEnvironment.getLockTimeoutInSeconds(props));
        final HazelcastInstance hz = HazelcastAccessor.getHazelcastInstance(sf);
        final Long id = new Long(1L);
        DummyEntity e = new DummyEntity(id, "", 0, null);
        Session session = sf.openSession();
        Transaction tx = session.beginTransaction();
        session.save(e);
        tx.commit();
        session.close();
        new Thread() {
            public void run() {
                final SessionFactoryImplementor sfi = (SessionFactoryImplementor) sf;
                final CacheKey key = new CacheKey(id, Hibernate.LONG,
                        DummyEntity.class.getName(), EntityMode.POJO, sfi);
                assertTrue(hz.getMap(DummyEntity.class.getName()).tryLock(key));
            }

            ;
        }.start();
        Thread.sleep(1000);
        session = sf.openSession();
        try {
            e = (DummyEntity) session.get(DummyEntity.class, id);
            e.setName("test");
            tx = session.beginTransaction();
            session.update(e);
            tx.commit();
        } finally {
            session.close();
            sf.close();
        }
    }

    private Properties getDefaultProperties() {
        Properties props = new Properties();
        props.setProperty(Environment.CACHE_PROVIDER, HazelcastCacheProvider.class.getName());
        props.setProperty(CacheEnvironment.CONFIG_FILE_PATH_LEGACY, "hazelcast-custom.xml");
        return props;
    }
}
