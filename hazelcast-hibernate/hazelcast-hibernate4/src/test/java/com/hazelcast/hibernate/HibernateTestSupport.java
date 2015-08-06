/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.entity.DummyEntity;
import com.hazelcast.hibernate.entity.DummyProperty;
import com.hazelcast.hibernate.instance.HazelcastAccessor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastTestSupport;
import org.hibernate.SessionFactory;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.URL;
import java.util.Properties;

public abstract class HibernateTestSupport extends HazelcastTestSupport{

    private final ILogger logger = Logger.getLogger(getClass());

    @BeforeClass
    @AfterClass
    public static void tearUpAndDown() {
        Hazelcast.shutdownAll();
    }

    protected String getCacheStrategy() {
        return  AccessType.READ_WRITE.getExternalName();
    }

    @After
    public final void cleanup() {
        Hazelcast.shutdownAll();
    }

    protected void sleep(int seconds) {
        try {
            Thread.sleep(1000 * seconds);
        } catch (InterruptedException e) {
            logger.warning("", e);
        }
    }

    protected SessionFactory createSessionFactory(Properties props) {
        Configuration conf = new Configuration();
        URL xml = HibernateTestSupport.class.getClassLoader().getResource("test-hibernate.cfg.xml");
        props.put(CacheEnvironment.EXPLICIT_VERSION_CHECK, "true");
        conf.configure(xml);
        conf.setCacheConcurrencyStrategy(DummyEntity.class.getName(), getCacheStrategy());
        conf.setCacheConcurrencyStrategy(DummyProperty.class.getName(), getCacheStrategy());
        conf.setCollectionCacheConcurrencyStrategy(DummyEntity.class.getName() + ".properties", getCacheStrategy());
        conf.addProperties(props);
        final SessionFactory sf = conf.buildSessionFactory();
        sf.getStatistics().setStatisticsEnabled(true);
        return sf;
    }

    protected HazelcastInstance getHazelcastInstance(SessionFactory sf) {
        return HazelcastAccessor.getHazelcastInstance(sf);
    }

}
