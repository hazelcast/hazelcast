/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.hibernate.entity.DummyEntity;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.cfg.Environment;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Date;
import java.util.Properties;

@RunWith(TestBlockJUnit4ClassRunner.class)
public class RegionFactoryDefaultTest extends HibernateStatisticsTestSupport {

    @BeforeClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    protected Properties getCacheProperties() {
        Properties props = new Properties();
        props.setProperty(Environment.CACHE_REGION_FACTORY, HazelcastCacheRegionFactory.class.getName());
        return props;
    }

    @Test
    public void testInsertGetUpdateGet() {
        Session session = sf.openSession();
        DummyEntity e = new DummyEntity(1L, "test", 0d, null);
        Transaction tx = session.beginTransaction();
        try {
            session.save(e);
            tx.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
            tx.rollback();
            Assert.fail(ex.getMessage());
        } finally {
            session.close();
        }

        session = sf.openSession();
        try {
            e = (DummyEntity) session.get(DummyEntity.class, 1L);
            Assert.assertEquals("test", e.getName());
            Assert.assertNull(e.getDate());
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        } finally {
            session.close();
        }

        session = sf.openSession();
        tx = session.beginTransaction();
        try {
            e = (DummyEntity) session.get(DummyEntity.class, 1L);
            Assert.assertEquals("test", e.getName());
            Assert.assertNull(e.getDate());
            e.setName("dummy");
            e.setDate(new Date());
            session.update(e);
            tx.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
            tx.rollback();
            Assert.fail(ex.getMessage());
        } finally {
            session.close();
        }

        session = sf.openSession();
        try {
            e = (DummyEntity) session.get(DummyEntity.class, 1L);
            Assert.assertEquals("dummy", e.getName());
            Assert.assertNotNull(e.getDate());
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        } finally {
            session.close();
        }
//        stats.logSummary();
    }
}
