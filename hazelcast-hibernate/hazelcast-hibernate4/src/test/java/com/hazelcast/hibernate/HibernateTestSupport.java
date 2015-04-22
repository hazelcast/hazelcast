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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastTestSupport;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.URL;
import java.util.Properties;

public abstract class HibernateTestSupport extends HazelcastTestSupport {

    private final ILogger logger = Logger.getLogger(getClass());

    @BeforeClass
    @AfterClass
    public static void tearUpAndDown() {
        Hazelcast.shutdownAll();
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

    protected static SessionFactory createSessionFactory(Properties props) {
        Configuration conf = new Configuration();
        URL xml = HibernateTestSupport.class.getClassLoader().getResource("test-hibernate.cfg.xml");
        props.put(CacheEnvironment.EXPLICIT_VERSION_CHECK, "true");
        conf.addProperties(props);
        conf.configure(xml);
        final SessionFactory sf = conf.buildSessionFactory();
        sf.getStatistics().setStatisticsEnabled(true);
        return sf;
    }
}
