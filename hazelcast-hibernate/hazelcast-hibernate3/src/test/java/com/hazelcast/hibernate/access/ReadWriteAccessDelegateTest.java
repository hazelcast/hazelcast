package com.hazelcast.hibernate.access;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.hibernate.HibernateTestSupport;
import com.hazelcast.hibernate.RegionCache;
import com.hazelcast.hibernate.region.HazelcastRegion;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.log4j.Level;
import org.hibernate.cache.access.SoftLock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ReadWriteAccessDelegateTest extends HibernateTestSupport {

    private RegionCache cache;
    private ReadWriteAccessDelegate<HazelcastRegion> delegate;

    @BeforeClass
    public static void setLogging() {
        setLoggingLog4j();
        setLogLevel(Level.TRACE);
    }

    @AfterClass
    public static void resetLogging() {
        resetLogLevel();
    }

    @Before
    public void setUp() {
        ILogger log = Logger.getLogger(ReadWriteAccessDelegateTest.class);

        cache = mock(RegionCache.class);

        HazelcastRegion region = mock(HazelcastRegion.class);
        when(region.getLogger()).thenReturn(log);
        when(region.getCache()).thenReturn(cache);

        delegate = new ReadWriteAccessDelegate<HazelcastRegion>(region, null);
    }

    @Test
    public void testAfterInsert() {
        when(cache.insert(any(), any(), any())).thenThrow(new HazelcastException("expected exception"));
        assertFalse(delegate.afterInsert(null, null, null));
    }

    @Test
    public void testAfterUpdate() {
        when(cache.update(any(), any(), any(), any(SoftLock.class))).thenThrow(new HazelcastException("expected exception"));
        assertFalse(delegate.afterUpdate(null, null, null, null, null));
    }
}
