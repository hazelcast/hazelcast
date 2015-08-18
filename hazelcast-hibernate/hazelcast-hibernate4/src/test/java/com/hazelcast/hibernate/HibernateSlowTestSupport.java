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
import com.hazelcast.hibernate.entity.DummyEntity;
import com.hazelcast.hibernate.entity.DummyProperty;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.junit.After;
import org.junit.Before;

import java.util.Date;
import java.util.List;
import java.util.Properties;

public abstract class HibernateSlowTestSupport extends HibernateTestSupport {

    protected SessionFactory sf;
    protected SessionFactory sf2;

    @Before
    public void postConstruct() {
        sf = createSessionFactory(getCacheProperties(),null);
        sf2 = createSessionFactory(getCacheProperties(),null);
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

    protected List<DummyEntity> executeQuery(SessionFactory factory) {
        Session session = factory.openSession();
        try {
            Query query = session.createQuery("from " + DummyEntity.class.getName());
            query.setCacheable(false);
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
}
