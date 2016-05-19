package com.hazelcast.cache.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cache.impl.JCacheDetector.isJcacheAvailable;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class JCacheDetectorTest extends HazelcastTestSupport {

    private ILogger logger = Logger.getLogger(JCacheDetectorTest.class);
    private ClassLoader classLoader = mock(ClassLoader.class);

    private Class instance;

    @Before
    public void setUp() throws Exception {
        instance = Class.forName("java.lang.Object");
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(JCacheDetector.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testIsJCacheAvailable_withCorrectVersion() throws Exception {
        when(classLoader.loadClass(anyString())).thenReturn(instance);

        assertTrue(isJcacheAvailable(classLoader));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testIsJCacheAvailable_withCorrectVersion_withLogger() throws Exception {
        when(classLoader.loadClass(anyString())).thenReturn(instance);

        assertTrue(isJcacheAvailable(classLoader, logger));
    }

    @Test
    public void testIsJCacheAvailable_notFound() throws Exception {
        assertFalse(isJcacheAvailable(classLoader));
    }

    @Test
    public void testIsJCacheAvailable_notFound_withLogger() throws Exception {
        assertFalse(isJcacheAvailable(classLoader, logger));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testIsJCacheAvailable_withWrongJCacheVersion() throws Exception {
        when(classLoader.loadClass(anyString())).thenReturn(instance).thenReturn(null);

        assertFalse(isJcacheAvailable(classLoader));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testIsJCacheAvailable_withWrongJCacheVersion_withLogger() throws Exception {
        when(classLoader.loadClass(anyString())).thenReturn(instance).thenReturn(null);

        assertFalse(isJcacheAvailable(classLoader, logger));
    }
}
