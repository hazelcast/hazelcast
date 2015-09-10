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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.hibernate.entity.DummyEntity;
import com.hazelcast.hibernate.entity.DummyProperty;
import com.hazelcast.hibernate.instance.HazelcastMockInstanceLoader;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.stat.SecondLevelCacheStatistics;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public abstract class HibernateStatisticsTestSupport extends HibernateTestSupport {

    protected SessionFactory sf;
    protected SessionFactory sf2;

    protected final String CACHE_ENTITY = DummyEntity.class.getName();
    protected final String CACHE_PROPERTY = DummyProperty.class.getName();
    private static TestHazelcastFactory factory;

    @Before
    public void postConstruct() {
        HazelcastMockInstanceLoader loader = new HazelcastMockInstanceLoader();
        factory = new TestHazelcastFactory();
        loader.setInstanceFactory(factory);
        sf = createSessionFactory(getCacheProperties(),  loader);
        sf2 = createSessionFactory(getCacheProperties(), loader);
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
            ignore(e);
        } finally {
            session.close();
        }
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

    protected ArrayList<DummyEntity> getDummyEntities(SessionFactory sf, long untilId) {
        Session session = sf.openSession();
        ArrayList<DummyEntity> entities = new ArrayList<DummyEntity>();
        for (long i=0; i<untilId; i++) {
            DummyEntity entity = (DummyEntity)session.get(DummyEntity.class, i);
            if (entity != null) {
                session.evict(entity);
                entities.add(entity);
            }
        }
        session.close();
        return entities;
    }

    protected Set<DummyProperty> getPropertiesOfEntity(SessionFactory sf, long entityId) {
        Session session = sf.openSession();
        DummyEntity entity = (DummyEntity)session.get(DummyEntity.class, entityId);
        if(entity != null) {
            return entity.getProperties();
        } else {
            return null;
        }
    }

    protected void updateDummyEntityName(SessionFactory sf, long id, String newName) {
        Session session = null;
        Transaction txn = null;
        try {
            session = sf.openSession();
            txn = session.beginTransaction();
            DummyEntity entityToUpdate = (DummyEntity)session.get(DummyEntity.class, id);
            entityToUpdate.setName(newName);
            session.update(entityToUpdate);
            txn.commit();
        } catch (RuntimeException e) {
            txn.rollback();
            e.printStackTrace();
            throw e;
        } finally {
            session.close();
        }
    }

    protected void deleteDummyEntity(SessionFactory sf, long id)
            throws Exception {
        Session session = null;
        Transaction txn = null;
        try {
            session = sf.openSession();
            txn = session.beginTransaction();
            DummyEntity entityToDelete = (DummyEntity) session.get(DummyEntity.class, id);
            session.delete(entityToDelete);
            txn.commit();
        } catch (Exception e) {
            txn.rollback();
            e.printStackTrace();
            throw e;
        } finally {
            session.close();
        }
    }

    protected void executeUpdateQuery(SessionFactory sf, String queryString)
            throws RuntimeException {
        Session session = null;
        Transaction txn = null;
        try {
            session = sf.openSession();
            txn = session.beginTransaction();
            Query query = session.createQuery(queryString);
            query.setCacheable(true);
            session.evict(query);
            query.executeUpdate();
            txn.commit();
        } catch (RuntimeException e) {
            txn.rollback();
            e.printStackTrace();
            throw e;
        } finally {
            session.close();
        }
    }

    @Test
    public void testUpdateQueryCausesInvalidationOfEntireRegion() {
        insertDummyEntities(10);

        executeUpdateQuery(sf, "UPDATE DummyEntity set name = 'manually-updated' where id=2");

        sf.getStatistics().clear();

        getDummyEntities(sf, 10);

        SecondLevelCacheStatistics dummyEntityCacheStats = sf.getStatistics().getSecondLevelCacheStatistics(CACHE_ENTITY);
        assertEquals(10, dummyEntityCacheStats.getMissCount());
        assertEquals(0, dummyEntityCacheStats.getHitCount());
    }
}
