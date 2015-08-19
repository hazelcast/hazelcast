package com.hazelcast.hibernate;

import com.hazelcast.hibernate.entity.DummyEntity;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.cfg.Environment;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class RegionFactoryQueryTest extends HibernateSlowTestSupport{

    @Before
    @Override
    public void postConstruct() {
        sf = createSessionFactory(getCacheProperties(), null);
        sf2 = createSessionFactory(getCacheProperties(), null);
    }

    @Test
    public void test_query_with_non_mock_network() {
        final int entityCount = 10;
        final int queryCount = 2;
        insertDummyEntities(entityCount);
        List<DummyEntity> list = null;
        for (int i = 0; i < queryCount; i++) {
            list = executeQuery(sf);
            assertEquals(entityCount, list.size());
        }
        for (int i = 0; i < queryCount; i++) {
            list = executeQuery(sf2);
            assertEquals(entityCount, list.size());
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
        assertEquals(entityCount - 1, executeQuery(sf).size());
        assertEquals(entityCount - 1, executeQuery(sf2).size());

    }

    protected Properties getCacheProperties() {
        Properties props = new Properties();
        props.setProperty(Environment.CACHE_REGION_FACTORY, HazelcastCacheRegionFactory.class.getName());
        return props;
    }
}
