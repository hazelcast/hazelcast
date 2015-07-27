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

import com.hazelcast.hibernate.access.ReadOnlyAccessDelegate;
import com.hazelcast.hibernate.entity.DummyEntity;
import com.hazelcast.hibernate.entity.DummyEntityNonStrictRW;
import com.hazelcast.hibernate.entity.DummyEntityReadOnly;
import com.hazelcast.hibernate.region.HazelcastRegion;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.cfg.Environment;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RegionFactoryDefaultTest extends HibernateStatisticsTestSupport {

    protected Properties getCacheProperties() {
        Properties props = new Properties();
        props.setProperty(Environment.CACHE_REGION_FACTORY, HazelcastCacheRegionFactory.class.getName());
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
        sf2.getCache().evictEntityRegions();

        session = sf.openSession();
        ArrayList<DummyEntityNonStrictRW> entities = new ArrayList<DummyEntityNonStrictRW>(entityCount);
        for (int i=0; i<entityCount; i++) {
            entities.add((DummyEntityNonStrictRW)session.get(DummyEntityNonStrictRW.class, (long)i));
        }
        assertEquals(entityCount*2, sf.getStatistics().getSecondLevelCacheMissCount());
        assertEquals(0, sf.getStatistics().getSecondLevelCacheHitCount());

        session.close();

        session = sf.openSession();
        txn = session.beginTransaction();
        DummyEntityNonStrictRW updatedEntity = entities.get(0);
        updatedEntity.setName("updated:" + 0);
        session.update(updatedEntity);
        DummyEntityNonStrictRW removedEntity = entities.get(1);
        session.delete(removedEntity);
        txn.commit();
        session.close();

        assertEquals(0, sf2.getStatistics().getSecondLevelCacheMissCount());
        assertEquals(0, sf2.getStatistics().getSecondLevelCacheHitCount());

        session = sf2.openSession();
        entities = new ArrayList<DummyEntityNonStrictRW>(entityCount);
        for (int i=0; i<entityCount; i++) {
            DummyEntityNonStrictRW ent = (DummyEntityNonStrictRW)session.get(DummyEntityNonStrictRW.class, (long)i);
            if(ent != null)
                entities.add(ent);
        }
        assertEquals(entityCount-1, entities.size());
        assertEquals(2 + childCount, sf2.getStatistics().getSecondLevelCacheMissCount());
        assertEquals((entityCount - 1) * (1 + childCount) + childCount, sf2.getStatistics().getSecondLevelCacheHitCount());
    }

    @Test
    public void testReadOnlyEntity() {
        Session session = null;
        Transaction txn = null;

        int entityCount = 10;
        int childCount = 4;

        insertDummyReadOnlyEntities(entityCount, childCount);

        sf.getCache().evictEntityRegions();
        sf2.getCache().evictEntityRegions();

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

        assertEquals(0, sf2.getStatistics().getSecondLevelCacheMissCount());
        assertEquals(0, sf2.getStatistics().getSecondLevelCacheHitCount());

        session = sf2.openSession();
        entities = new ArrayList<DummyEntityReadOnly>(entityCount);
        for (int i=0; i<entityCount; i++) {
            DummyEntityReadOnly ent = (DummyEntityReadOnly)session.get(DummyEntityReadOnly.class, (long)i);
            entities.add(ent);
            session.evict(ent);
        }
        //missed entity: deleted entity(id = 2)
        assertEquals(1, sf2.getStatistics().getSecondLevelCacheMissCount());
        //hit entitities: all entitties except the deleted one along with their children
        assertEquals((entityCount-1) * (2 + childCount), sf2.getStatistics().getSecondLevelCacheHitCount());
    }


    @Test
    public void testUpdateQueryCausesInvalidationOfEntireRegion() {
        int entityCount = 10;
        insertDummyEntities(entityCount);
        Session session = null;

        int res = executeUpdateQueryInTransaction(sf, "UPDATE DummyEntity set name = 'manually-updated' where id=2");

        assertEquals(1, res);

        sf.getStatistics().clear();

        session = sf.openSession();
        ArrayList<DummyEntity> entities = new ArrayList<DummyEntity>(entityCount);
        for (int i=0; i<entityCount; i++) {
            entities.add((DummyEntity)session.get(DummyEntity.class, (long)i));
        }
        assertEquals(entityCount * 2, sf.getStatistics().getSecondLevelCacheMissCount());
        assertEquals(0, sf.getStatistics().getSecondLevelCacheHitCount());
    }

    @Test
    public void testUpdateQueryOnNonStrictRWCausesInvalidationOfEntireRegion() {
        int entityCount = 10;
        insertDummyNonStrictRWEntities(entityCount, 0);
        Session session = null;

        int res = executeUpdateQueryInTransaction(sf, "UPDATE DummyEntityNonStrictRW set name = 'manually-updated' where id=2");
        assertEquals(1, res);
        sf.getStatistics().clear();

        session = sf.openSession();
        ArrayList<DummyEntityNonStrictRW> entities = new ArrayList<DummyEntityNonStrictRW>(entityCount);
        for (int i=0; i<entityCount; i++) {
            entities.add((DummyEntityNonStrictRW)session.get(DummyEntityNonStrictRW.class, (long) i));
        }
        assertEquals(entityCount * 2, sf.getStatistics().getSecondLevelCacheMissCount());
        assertEquals(0, sf.getStatistics().getSecondLevelCacheHitCount());
    }

    @Test
    public void testQueryRegionEviction() {
        sf.getCache().evictDefaultQueryRegion();
        int entityCount = 10;
        insertDummyEntities(entityCount, 0);
        List<DummyEntity> retrievedEntities = executeListQueryInTransaction(sf);
        assertEquals(entityCount, retrievedEntities.size());

        sf.getStatistics().clear();
        executeListQueryInTransaction(sf);

        assertEquals(1, sf.getStatistics().getQueryCacheHitCount());
        assertEquals(0, sf.getStatistics().getQueryCacheMissCount());

        executeUpdateQueryInTransaction(sf, "delete from DummyEntity");

        sf.getStatistics().clear();
        executeListQueryInTransaction(sf);
        executeListQueryInTransaction(sf);
        assertEquals(1, sf.getStatistics().getQueryCacheHitCount());
        assertEquals(1, sf.getStatistics().getQueryCacheMissCount());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testReadOnlyUpdate() throws Exception{
        Session session = null;
        Transaction txn = null;

        insertDummyReadOnlyEntities(1, 0);
        try {
            session = sf.openSession();
            DummyEntityReadOnly e = (DummyEntityReadOnly) session.get(DummyEntityReadOnly.class, (long)0);
            txn = session.beginTransaction();
            e.setName("updated");
            session.update(e);
            txn.commit();
        } catch (Exception e) {
            txn.rollback();
            throw e;
        } finally {
            session.close();
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAfterUpdateShouldThrowOnReadOnly() {
        HazelcastRegion hzRegion = mock(HazelcastRegion.class);
        when(hzRegion.getCache()).thenReturn(null);
        when(hzRegion.getLogger()).thenReturn(null);
        ReadOnlyAccessDelegate readOnlyAccessDelegate = new ReadOnlyAccessDelegate(hzRegion, null);
        readOnlyAccessDelegate.afterUpdate(null, null, null, null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLockRegionShouldThrowOnReadOnly() {
        HazelcastRegion hzRegion = mock(HazelcastRegion.class);
        when(hzRegion.getCache()).thenReturn(null);
        when(hzRegion.getLogger()).thenReturn(null);
        ReadOnlyAccessDelegate readOnlyAccessDelegate = new ReadOnlyAccessDelegate(hzRegion, null);
        readOnlyAccessDelegate.lockRegion();
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
            fail(ex.getMessage());
        } finally {
            session.close();
        }

        session = sf.openSession();
        try {
            e = (DummyEntity) session.get(DummyEntity.class, 1L);
            assertEquals("test", e.getName());
            assertNull(e.getDate());
        } catch (Exception ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        } finally {
            session.close();
        }

        session = sf.openSession();
        tx = session.beginTransaction();
        try {
            e = (DummyEntity) session.get(DummyEntity.class, 1L);
            assertEquals("test", e.getName());
            assertNull(e.getDate());
            e.setName("dummy");
            e.setDate(new Date());
            session.update(e);
            tx.commit();
        } catch (Exception ex) {
            ex.printStackTrace();
            tx.rollback();
            fail(ex.getMessage());
        } finally {
            session.close();
        }

        session = sf.openSession();
        try {
            e = (DummyEntity) session.get(DummyEntity.class, 1L);
            assertEquals("dummy", e.getName());
            Assert.assertNotNull(e.getDate());
        } catch (Exception ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        } finally {
            session.close();
        }
//        stats.logSummary();
    }
}
