/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache;

import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;

import static com.hazelcast.cache.CacheUtil.getPrefix;
import static com.hazelcast.cache.CacheUtil.getPrefixedCacheName;
import static com.hazelcast.cache.HazelcastCacheManager.CACHE_MANAGER_PREFIX;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheUtilTest extends HazelcastTestSupport {

    private static final String CACHE_NAME = "MY-CACHE";
    private static final String URI_SCOPE = "MY-SCOPE";
    private static final String CLASSLOADER_SCOPE = "MY-CLASSLOADER";

    @Parameter
    public URI uri;

    @Parameter(1)
    public ClassLoader classLoader;

    @Parameter(2)
    public String expectedPrefix;

    @Parameter(3)
    public String expectedPrefixedCacheName;

    @Parameter(4)
    public String expectedDistributedObjectName;

    // each entry is Object[] with
    // {URI, ClassLoader, expected prefix, expected prefixed cache name, expected distributed object name}
    @Parameters(name = "{index}: uri={0}, classLoader={1}")
    public static Collection<Object[]> parameters() throws URISyntaxException {
        final URI uri = new URI(URI_SCOPE);
        final ClassLoader classLoader = mock(ClassLoader.class);
        when(classLoader.toString()).thenReturn(CLASSLOADER_SCOPE);
        return asList(
                new Object[]{
                        null, null, null, CACHE_NAME, CACHE_MANAGER_PREFIX + CACHE_NAME,
                },
                new Object[]{
                        uri, null, URI_SCOPE + "/", URI_SCOPE + "/" + CACHE_NAME,
                        CACHE_MANAGER_PREFIX + URI_SCOPE + "/" + CACHE_NAME,
                },
                new Object[]{
                        null, classLoader, CLASSLOADER_SCOPE + "/", CLASSLOADER_SCOPE + "/" + CACHE_NAME,
                        CACHE_MANAGER_PREFIX + CLASSLOADER_SCOPE + "/" + CACHE_NAME,
                },
                new Object[]{
                        uri, classLoader, URI_SCOPE + "/" + CLASSLOADER_SCOPE + "/",
                        URI_SCOPE + "/" + CLASSLOADER_SCOPE + "/" + CACHE_NAME,
                        CACHE_MANAGER_PREFIX + URI_SCOPE + "/" + CLASSLOADER_SCOPE + "/" + CACHE_NAME,
                }
        );
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(CacheUtil.class);
    }

    @Test
    public void testGetPrefix() {
        String prefix = getPrefix(uri, classLoader);
        assertEquals(expectedPrefix, prefix);
    }

    @Test
    public void testGetPrefixedCacheName() {
        String prefix = getPrefixedCacheName(CACHE_NAME, uri, classLoader);
        assertEquals(expectedPrefixedCacheName, prefix);
    }

    @Test
    public void testGetDistributedObjectName() {
        String distributedObjectName = CacheUtil.getDistributedObjectName(CACHE_NAME, uri, classLoader);
        assertEquals(expectedDistributedObjectName, distributedObjectName);
    }

}
