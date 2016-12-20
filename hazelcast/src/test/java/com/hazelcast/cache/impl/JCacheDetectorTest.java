package com.hazelcast.cache.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static com.hazelcast.cache.impl.JCacheDetector.isJCacheAvailable;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ClassLoaderUtil.class)
@Category({QuickTest.class, ParallelTest.class})
public class JCacheDetectorTest extends HazelcastTestSupport {

    private ILogger logger = Logger.getLogger(JCacheDetectorTest.class);
    private ClassLoader classLoader = mock(ClassLoader.class);

    @Before
    public void setUp() {
        PowerMockito.mockStatic(ClassLoaderUtil.class);
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(JCacheDetector.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testIsJCacheAvailable_withCorrectVersion() throws Exception {
        when(ClassLoaderUtil.isClassAvailable(any(ClassLoader.class), anyString()))
                .thenReturn(true);

        assertTrue(isJCacheAvailable(classLoader));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testIsJCacheAvailable_withCorrectVersion_withLogger() throws Exception {
        when(ClassLoaderUtil.isClassAvailable(any(ClassLoader.class), anyString()))
                .thenReturn(true);

        assertTrue(isJCacheAvailable(classLoader, logger));
    }

    @Test
    public void testIsJCacheAvailable_notFound() throws Exception {
        when(ClassLoaderUtil.isClassAvailable(any(ClassLoader.class), anyString()))
                .thenReturn(false);
        assertFalse(isJCacheAvailable(classLoader));
    }

    @Test
    public void testIsJCacheAvailable_notFound_withLogger() throws Exception {
        when(ClassLoaderUtil.isClassAvailable(any(ClassLoader.class), anyString()))
                .thenReturn(false);
        assertFalse(isJCacheAvailable(classLoader, logger));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testIsJCacheAvailable_withWrongJCacheVersion() throws Exception {
        when(ClassLoaderUtil.isClassAvailable(any(ClassLoader.class), anyString()))
                .thenReturn(true)
                .thenReturn(false);

        assertFalse(isJCacheAvailable(classLoader));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testIsJCacheAvailable_withWrongJCacheVersion_withLogger() throws Exception {
        when(ClassLoaderUtil.isClassAvailable(any(ClassLoader.class), anyString()))
                .thenReturn(true)
                .thenReturn(false);
        assertFalse(isJCacheAvailable(classLoader, logger));
    }
}
