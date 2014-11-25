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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.entity.DummyEntity;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.cfg.Environment;
import org.hibernate.stat.Statistics;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class LocalRegionFactoryDefaultTest extends RegionFactoryDefaultTest {

    @BeforeClass
    @AfterClass
    public static void killAllHazelcastInstances() throws IOException {
        Hazelcast.shutdownAll();
    }

    protected Properties getCacheProperties() {
        Properties props = new Properties();
        props.setProperty(Environment.CACHE_REGION_FACTORY, HazelcastLocalCacheRegionFactory.class.getName());
        return props;
    }

    @Test
    public void testEntity() {
        final HazelcastInstance hz = getHazelcastInstance(sf);
        assertNotNull(hz);
        final int count = 100;
        final int childCount = 3;
        insertDummyEntities(count, childCount);
        sleep(1);
        List<DummyEntity> list = new ArrayList<DummyEntity>(count);
        Session session = sf.openSession();
        try {
            for (int i = 0; i < count; i++) {
                DummyEntity e = (DummyEntity) session.get(DummyEntity.class, (long) i);
                session.evict(e);
                list.add(e);
            }
        } finally {
            session.close();
        }
        session = sf.openSession();
        Transaction tx = session.beginTransaction();
        try {
            for (DummyEntity dummy : list) {
                dummy.setDate(new Date());
                session.update(dummy);
            }
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }

        Statistics stats = sf.getStatistics();
        assertEquals((childCount + 1) * count, stats.getEntityInsertCount());
        // twice put of entity and properties (on load and update) and once put of collection
        assertEquals((childCount + 1) * count * 2 + count, stats.getSecondLevelCachePutCount());
        assertEquals(childCount * count, stats.getEntityLoadCount());
        assertEquals(count, stats.getSecondLevelCacheHitCount());
        // collection cache miss
        assertEquals(count, stats.getSecondLevelCacheMissCount());
        stats.logSummary();
    }
}
