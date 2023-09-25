/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.it.hibernate;

import com.hazelcast.hibernate.HazelcastCacheRegionFactory;
import com.hazelcast.hibernate.HazelcastLocalCacheRegionFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.stat.Statistics;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class HibernateIT extends HazelcastTestSupport {

    private static final ILogger log = Logger.getLogger(HibernateIT.class);

    @Parameterized.Parameter
    public Class<?> cacheRegionFactoryClass;

    @Parameterized.Parameters(name = "cacheRegionFactory: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {HazelcastCacheRegionFactory.class},
                {HazelcastLocalCacheRegionFactory.class},
        });
    }

    private Process process;
    private String clusterName;
    private SessionFactory sessionFactory;

    @Before
    public void setUp() throws Exception {
        assumeThatNoWindowsOS();
        clusterName = this.getClass().getSimpleName() + "_" + UUID.randomUUID();
        startHzMember();
        sessionFactory = createSessionFactory(getCacheProperties(cacheRegionFactoryClass.getName()));
    }

    @Test
    public void testInsertLoad() {
        Session session = sessionFactory.openSession();
        Transaction tx = session.beginTransaction();
        AnnotatedEntity e = new AnnotatedEntity("some-title");
        session.save(e);
        tx.commit();
        session.close();


        Statistics stats = sessionFactory.getStatistics();
        assertThat(stats.getEntityInsertCount()).isEqualTo(1);
        assertThat(stats.getSecondLevelCachePutCount()).isEqualTo(1);

        // The assertion below can fail the first time on macOS docker-desktop due to small (1-2ms) clock drifts on VM.
        // See https://github.com/docker/for-mac/issues/2076
        assertTrueEventually(() -> {
            stats.clear();
            for (int i = 0; i < 10; i++) {
                Session session2 = sessionFactory.openSession();
                AnnotatedEntity retrieved = session2.get(AnnotatedEntity.class, (long) 1);
                assertThat(retrieved.getTitle()).isEqualTo("some-title");
                session2.close();
            }
            assertThat(stats.getSecondLevelCacheHitCount()).isEqualTo(10);
        });
    }

    private SessionFactory createSessionFactory(Properties props) {
        Configuration conf = new Configuration();
        conf.configure(HibernateIT.class.getClassLoader().getResource("test-hibernate-client.cfg.xml"));
        conf.addAnnotatedClass(AnnotatedEntity.class);
        conf.addProperties(props);

        final SessionFactory sessionFactory = conf.buildSessionFactory();
        sessionFactory.getStatistics().setStatisticsEnabled(true);
        return sessionFactory;
    }

    private Properties getCacheProperties(String cacheRegionFactory) {
        Properties props = new Properties();
        props.setProperty(Environment.CACHE_REGION_FACTORY, cacheRegionFactory);
        props.setProperty("hibernate.cache.hazelcast.native_client_cluster_name", clusterName);
        return props;
    }

    private void startHzMember() throws IOException {
        ProcessBuilder builder = new ProcessBuilder("bin/hz", "start")
                .directory(new File("./target/hazelcast"))
                .inheritIO();
        builder.environment().put("HZ_CLUSTERNAME", clusterName);
        builder.environment().put("JAVA_OPTS", "-Dhazelcast.phone.home.enabled=false");
        // Remove classpath set by Maven
        builder.environment().remove("CLASSPATH");

        process = builder.start();
    }

    @After
    public void tearDown() throws Exception {
        if (sessionFactory != null) {
            sessionFactory.close();
        }
        log.info("Destroying Hazelcast process");
        process.destroy();
        boolean destroyed = process.waitFor(30, TimeUnit.SECONDS);
        if (!destroyed) {
            log.info("Hazelcast process not destroyed, trying Process#destroyForcibly()");
            process.destroyForcibly()
                    .waitFor();
        }
    }

}
