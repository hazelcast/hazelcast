package com.hazelcast.cache;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.URI;

import static com.hazelcast.cache.CacheUtil.getPrefix;
import static com.hazelcast.cache.CacheUtil.getPrefixedCacheName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheUtilTest extends HazelcastTestSupport {

    private URI uri;
    private ClassLoader classLoader;

    @Before
    public void setUp() throws Exception {
        uri = new URI("MY-SCOPE");
        classLoader = mock(ClassLoader.class);
        when(classLoader.toString()).thenReturn("MY-CLASSLOADER");
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(CacheUtil.class);
    }

    @Test
    public void testGetPrefix() {
        String prefix = getPrefix(uri, classLoader);
        assertEquals("MY-SCOPE/MY-CLASSLOADER/", prefix);
    }

    @Test
    public void testGetPrefix_whenClassLoaderIsNull() {
        String prefix = getPrefix(uri, null);
        assertEquals("MY-SCOPE/", prefix);
    }

    @Test
    public void testGetPrefix_whenUriIsNull() {
        String prefix = getPrefix(null, classLoader);
        assertEquals("MY-CLASSLOADER/", prefix);
    }

    @Test
    public void testGetPrefix_whenUriAndClassLoaderAreNull() {
        assertNull(getPrefix(null, null));
    }

    @Test
    public void testGetPrefixedCacheName() {
        String prefix = getPrefixedCacheName("MY-NAME", uri, classLoader);
        assertEquals("MY-SCOPE/MY-CLASSLOADER/MY-NAME", prefix);
    }

    @Test
    public void testGetPrefixedCacheName_whenClassLoaderIsNull() {
        String prefix = getPrefixedCacheName("MY-NAME", uri, null);
        assertEquals("MY-SCOPE/MY-NAME", prefix);
    }

    @Test
    public void testGetPrefixedCacheName_whenUriIsNull() {
        String prefix = getPrefixedCacheName("MY-NAME", null, classLoader);
        assertEquals("MY-CLASSLOADER/MY-NAME", prefix);
    }

    @Test
    public void testGetPrefixedCacheName_whenUriAndClassLoaderAreNull() {
        String prefix = getPrefixedCacheName("MY-NAME", null, null);
        assertEquals("MY-NAME", prefix);
    }
}
