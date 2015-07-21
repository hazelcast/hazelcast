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

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.entity.DummyEntity;
import com.hazelcast.hibernate.entity.DummyEntityNonStrictRW;
import com.hazelcast.hibernate.entity.DummyEntityReadOnly;
import com.hazelcast.hibernate.entity.DummyProperty;
import com.hazelcast.hibernate.entity.DummyPropertyNonStrictRW;
import com.hazelcast.hibernate.entity.DummyPropertyReadOnly;
import com.hazelcast.hibernate.instance.HazelcastAccessor;
import com.hazelcast.hibernate.region.HazelcastQueryResultsRegion;
import com.hazelcast.test.annotation.NightlyTest;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.impl.SessionFactoryImpl;
import org.hibernate.stat.Statistics;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public abstract class HibernateStatisticsTestSupport extends HibernateTestSupport {

    protected SessionFactory sf;
    protected SessionFactory sf2;

    @Before
    public void postConstruct() {
        sf = createSessionFactory(getCacheProperties());
        sf2 = createSessionFactory(getCacheProperties());
    }

    @After
    public void preDestroy() {
        if (sf != null) {
            sf.close();
            sf = null;
        }
        if (sf2 != null) {
            sf2.close();
            sf2 = null;
        }
        Hazelcast.shutdownAll();
    }

    protected HazelcastInstance getHazelcastInstance(SessionFactory sf) {
        return HazelcastAccessor.getHazelcastInstance(sf);
    }

    protected abstract Properties getCacheProperties();

    protected void insertDummyEntities(int count) {
        insertDummyEntities(count, 0);
    }

    protected void insertDummyEntities(int count, int childCount) {
        Session session = sf.openSession();
        Transaction tx = session.beginTransaction();
        try {
            for (int i = 0; i < count; i++) {
                DummyEntity e = new DummyEntity((long) i, "dummy:" + i, i * 123456d, new Date());
                session.save(e);
                for (int j = 0; j < childCount; j++) {
                    DummyProperty p = new DummyProperty("key:" + j, e);
                    session.save(p);
                }
            }
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
    }

    protected void insertDummyNonStrictRWEntities(int count, int childCount) {
        Session session = sf.openSession();
        Transaction tx = session.beginTransaction();
        try {
            for (int i = 0; i < count; i++) {
                DummyEntityNonStrictRW e = new DummyEntityNonStrictRW((long) i, "dummy:" + i, i * 123456d, new Date());
                session.save(e);
                for (int j = 0; j < childCount; j++) {
                    DummyPropertyNonStrictRW p = new DummyPropertyNonStrictRW("key:" + j, e);
                    session.save(p);
                }
            }
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
    }

    protected void insertDummyReadOnlyEntities(int count, int childCount) {
        Session session = sf.openSession();
        Transaction tx = session.beginTransaction();
        try {
            for (int i = 0; i < count; i++) {
                DummyEntityReadOnly e = new DummyEntityReadOnly((long) i, "dummy:" + i, i * 123456d, new Date());
                session.save(e);
                for (int j = 0; j < childCount; j++) {
                    DummyPropertyReadOnly p = new DummyPropertyReadOnly("key:" + j, e);
                    session.save(p);
                }
            }
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
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
        sleep(1);


        assertEquals(0, sf2.getStatistics().getSecondLevelCacheMissCount());
        assertEquals(0, sf2.getStatistics().getSecondLevelCacheHitCount());

        session = sf2.openSession();
        entities = new ArrayList<DummyEntityNonStrictRW>(entityCount);
        for (int i=0; i<entityCount; i++) {
            DummyEntityNonStrictRW ent = (DummyEntityNonStrictRW)session.get(DummyEntityNonStrictRW.class, (long)i);
            entities.add(ent);
        }
        //missed entries: updated entry, children of updated entry, deleted entry
        assertEquals(1 + 1 + childCount, sf2.getStatistics().getSecondLevelCacheMissCount());
        //assertEquals((entityCount - 1) * (2 + childCount), sf2.getStatistics().getSecondLevelCacheHitCount());

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
        Map<?, ?> cache = hz.getMap(DummyEntity.class.getName());
        Map<?, ?> propCache = hz.getMap(DummyProperty.class.getName());
        Map<?, ?> propCollCache = hz.getMap(DummyEntity.class.getName() + ".properties");
        assertEquals((childCount + 1) * count, stats.getEntityInsertCount());
        // twice put of entity and properties (on load and update) and once put of collection
        // TODO: fix next assertion ->
//        assertEquals((childCount + 1) * count * 2, stats.getSecondLevelCachePutCount());
        assertEquals(childCount * count, stats.getEntityLoadCount());
        assertEquals(count, stats.getSecondLevelCacheHitCount());
        // collection cache miss
        assertEquals(count, stats.getSecondLevelCacheMissCount());
        assertEquals(count, cache.size());
        assertEquals(count * childCount, propCache.size());
        assertEquals(count, propCollCache.size());
        sf.getCache().evictEntityRegion(DummyEntity.class);
        sf.getCache().evictEntityRegion(DummyProperty.class);
        assertEquals(0, cache.size());
        assertEquals(0, propCache.size());
        stats.logSummary();
    }

    @Test
    public void testQuery() {
        final int entityCount = 10;
        final int queryCount = 3;
        insertDummyEntities(entityCount);
        sleep(2);
        List<DummyEntity> list = null;
        for (int i = 0; i < queryCount; i++) {
            list = executeQuery(sf);
            assertEquals(entityCount, list.size());
            sleepAtLeastSeconds(1);
        }

        assertNotNull(list);
        Session session = sf.openSession();
        Transaction tx = session.beginTransaction();
        try {
            for (DummyEntity dummy : list) {
                session.delete(dummy);
            }
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }

        Statistics stats = sf.getStatistics();
        assertEquals(1, stats.getQueryCachePutCount());
        assertEquals(1, stats.getQueryCacheMissCount());
        assertEquals(queryCount - 1, stats.getQueryCacheHitCount());
        assertEquals(1, stats.getQueryExecutionCount());
        assertEquals(entityCount, stats.getEntityInsertCount());
//      FIXME
//      HazelcastRegionFactory puts into L2 cache 2 times; 1 on insert, 1 on query execution 
//      assertEquals(entityCount, stats.getSecondLevelCachePutCount());
        assertEquals(entityCount, stats.getEntityLoadCount());
        assertEquals(entityCount, stats.getEntityDeleteCount());
        assertEquals(entityCount * (queryCount - 1) * 2, stats.getSecondLevelCacheHitCount());
        // collection cache miss
        assertEquals(entityCount, stats.getSecondLevelCacheMissCount());

        stats.logSummary();
    }

    protected List<DummyEntity> executeQuery(SessionFactory factory) {
        Session session = factory.openSession();
        try {
            Query query = session.createQuery("from " + DummyEntity.class.getName());
            query.setCacheable(true);
            return query.list();
        } finally {
            session.close();
        }
    }

    protected DummyEntity executeQuery(SessionFactory factory, long id) {
        Session session = factory.openSession();
        try {
            Query query = session.createQuery("from " + DummyEntity.class.getName() + " where id = " + id);
            query.setCacheable(true);
            return (DummyEntity) query.list().get(0);
        } finally {
            session.close();
        }
    }

    protected int executeUpdateQueryInTransaction(SessionFactory factory, String queryString) {
        Session session = factory.openSession();
        Transaction txn = null;
        int res = -1;
        try {
            txn = session.beginTransaction();
            Query query = session.createQuery(queryString);

            res = query.executeUpdate();
            txn.commit();
        } finally {
            session.close();
        }
        return res;
    }

    @Test
    public void testQuery2() {
        final int entityCount = 10;
        final int queryCount = 2;
        insertDummyEntities(entityCount);
        sleep(1);
        List<DummyEntity> list = null;
        for (int i = 0; i < queryCount; i++) {
            list = executeQuery(sf);
            assertEquals(entityCount, list.size());
            sleep(1);
        }

        for (int i = 0; i < queryCount; i++) {
            list = executeQuery(sf2);
            assertEquals(entityCount, list.size());
            sleep(1);
        }

        assertNotNull(list);
        DummyEntity toDelete = list.get(0);
        Session session = sf.openSession();
        Transaction tx = session.beginTransaction();
        try {
            session.delete(toDelete);
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
        sleep(1);
        assertEquals(entityCount - 1, executeQuery(sf).size());
        assertEquals(entityCount - 1, executeQuery(sf2).size());
    }

    @Test
    @Category(NightlyTest.class)
    public void testQueryCacheCleanup() {

        MapConfig mapConfig = getHazelcastInstance(sf).getConfig().getMapConfig("org.hibernate.cache.*");
        final float baseEvictionRate = 0.2f;
        final int numberOfEntities = 100;
        final int defaultCleanupPeriod = 60;
        final int maxSize = mapConfig.getMaxSizeConfig().getSize();
        final int evictedItemCount = numberOfEntities - maxSize + (int) (maxSize * baseEvictionRate);
        insertDummyEntities(numberOfEntities);
        sleep(1);
        for (int i = 0; i < numberOfEntities; i++) {
            executeQuery(sf, i);
        }

        HazelcastQueryResultsRegion queryRegion = ((HazelcastQueryResultsRegion) (((SessionFactoryImpl) sf).getQueryCache()).getRegion());
        assertEquals(numberOfEntities, queryRegion.getCache().size());
        sleep(defaultCleanupPeriod);

        assertEquals(numberOfEntities - evictedItemCount, queryRegion.getCache().size());
    }
}
