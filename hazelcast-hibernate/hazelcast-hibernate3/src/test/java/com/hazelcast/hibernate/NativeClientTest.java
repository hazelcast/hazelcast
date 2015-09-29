package com.hazelcast.hibernate;

import com.hazelcast.hibernate.entity.DummyEntity;
import com.hazelcast.hibernate.entity.DummyProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.Session;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.URL;
import java.util.Date;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class NativeClientTest
        extends HibernateSlowTestSupport {

    protected SessionFactory clientSf;

    @Override
    protected Properties getCacheProperties() {
        Properties props = new Properties();
        props.setProperty(Environment.CACHE_REGION_FACTORY, HazelcastCacheRegionFactory.class.getName());
        return props;
    }

    @Before
    @Override
    public void postConstruct() {
        sf = createSessionFactory(getCacheProperties(), null);
        clientSf = createClientSessionFactory(getCacheProperties());
    }

    @Test
    public void testInsertLoad() {
        Session session = clientSf.openSession();
        Transaction tx = session.beginTransaction();
        DummyEntity e = new DummyEntity((long) 1, "dummy:1", 123456d, new Date());
        session.save(e);
        tx.commit();
        session.close();

        session = clientSf.openSession();
        DummyEntity retrieved = (DummyEntity) session.get(DummyEntity.class, (long)1);
        assertEquals("dummy:1", retrieved.getName());
    }

    @After
    public void tearDown() {
        if(clientSf !=null) {
            clientSf.close();
        }
    }

    protected SessionFactory createClientSessionFactory(Properties props) {
        Configuration conf = new Configuration();
        URL xml = HibernateTestSupport.class.getClassLoader().getResource("test-hibernate-client.cfg.xml");
        conf.configure(xml);
        conf.setCacheConcurrencyStrategy(DummyEntity.class.getName(), getCacheStrategy());
        conf.setCacheConcurrencyStrategy(DummyProperty.class.getName(), getCacheStrategy());
        conf.setCollectionCacheConcurrencyStrategy(DummyEntity.class.getName() + ".properties", getCacheStrategy());
        conf.addProperties(props);
        final SessionFactory sf = conf.buildSessionFactory();
        sf.getStatistics().setStatisticsEnabled(true);
        return sf;
    }

}
