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
import com.hazelcast.hibernate.entity.DummyEntityNonStrictRW;
import com.hazelcast.hibernate.entity.DummyEntityReadOnly;
import com.hazelcast.hibernate.entity.DummyProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
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
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
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
    public void testNonStrictReadWriteEntity() {
        Session session = null;
        Transaction txn = null;
        int entityCount = 10;
        int childCount = 4;

        insertDummyNonStrictRWEntities(entityCount, childCount);

        sf.getCache().evictEntityRegions();

        session = sf.openSession();
        ArrayList<DummyEntityNonStrictRW> entities = new ArrayList<DummyEntityNonStrictRW>(entityCount);
        for (int i=0; i<entityCount; i++) {
            entities.add((DummyEntityNonStrictRW)session.get(DummyEntityNonStrictRW.class, (long)i));
        }
        assertEquals(entityCount*2, sf.getStatistics().getSecondLevelCacheMissCount());
        assertEquals(0, sf.getStatistics().getSecondLevelCacheHitCount());

        session.close();
        sf.getStatistics().clear();

        session = sf.openSession();
        txn = session.beginTransaction();
        DummyEntityNonStrictRW updatedEntity = entities.get(0);
        updatedEntity.setName("updated:" + 0);
        session.update(updatedEntity);
        DummyEntityNonStrictRW removedEntity = entities.get(1);
        session.delete(removedEntity);
        txn.commit();
        session.close();

        session = sf.openSession();
        entities = new ArrayList<DummyEntityNonStrictRW>(entityCount);
        for (int i=0; i<entityCount; i++) {
            DummyEntityNonStrictRW ent = (DummyEntityNonStrictRW)session.get(DummyEntityNonStrictRW.class, (long)i);
            if(ent != null)
                entities.add(ent);
        }
        assertEquals(entityCount-1, entities.size());
        assertEquals(2 + childCount, sf.getStatistics().getSecondLevelCacheMissCount());
        assertEquals((entityCount - 1) * (1 + childCount) + childCount, sf.getStatistics().getSecondLevelCacheHitCount());
    }

    @Test
    public void testReadOnlyEntity() {
        Session session = null;
        Transaction txn = null;

        int entityCount = 10;
        int childCount = 4;

        insertDummyReadOnlyEntities(entityCount, childCount);

        sf.getCache().evictEntityRegions();

        session = sf.openSession();
        ArrayList<DummyEntityReadOnly> entities = new ArrayList<DummyEntityReadOnly>(entityCount);
        for (int i=0; i<entityCount; i++) {
            entities.add((DummyEntityReadOnly)session.get(DummyEntityReadOnly.class, (long)i));
        }
        assertEquals(entityCount * 2, sf.getStatistics().getSecondLevelCacheMissCount());
        assertEquals(0, sf.getStatistics().getSecondLevelCacheHitCount());


        session.close();

        session = sf.openSession();
        txn =session.beginTransaction();
        DummyEntityReadOnly deletedEntity = (DummyEntityReadOnly) session.get(DummyEntityReadOnly.class, (long) 2);
        session.delete(deletedEntity);
        txn.commit();

        sf.getStatistics().clear();

        session = sf.openSession();
        entities = new ArrayList<DummyEntityReadOnly>(entityCount);
        for (int i=0; i<entityCount; i++) {
            DummyEntityReadOnly ent = (DummyEntityReadOnly)session.get(DummyEntityReadOnly.class, (long)i);
            entities.add(ent);
            session.evict(ent);
        }
        //missed entity: deleted entity(id = 2)
        assertEquals(1, sf.getStatistics().getSecondLevelCacheMissCount());
        //hit entitities: all entitties except the deleted one along with their children
        assertEquals((entityCount-1) * (2 + childCount), sf.getStatistics().getSecondLevelCacheHitCount());
    }

    @Test
    public void testReadWriteEntity() {
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

    @Test
    public void testEntityPropertyUpdate() {
        final int count = 100;
        final int childCount = 3;
        insertDummyEntities(count, childCount);
        sleep(1);
        Session session = sf.openSession();

        DummyEntity e1 = (DummyEntity) session.get(DummyEntity.class, (long) 1);
        e1.setDate(new Date());
        Set<DummyProperty> dummyProperties = e1.getProperties();


        Transaction tx = session.beginTransaction();
        try {
            for (DummyProperty property : dummyProperties) {
                property.setKey("test");
                session.update(property);
            }
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }

        session = sf2.openSession();
        e1 = (DummyEntity) session.get(DummyEntity.class, (long) 1);
        Transaction tx2 = session.beginTransaction();
        try {
            e1.getProperties().clear();
            tx2.commit();
        } catch (Exception e) {
            tx2.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }


        Statistics stats = sf.getStatistics();
        stats.logSummary();
    }
}
