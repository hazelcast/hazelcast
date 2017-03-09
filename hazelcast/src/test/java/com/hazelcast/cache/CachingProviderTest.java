/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheException;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CachingProviderTest extends HazelcastTestSupport {

    protected static final int INSTANCE_COUNT = 2;

    protected TestHazelcastInstanceFactory instanceFactory;

    @Before
    public void setup() {
        instanceFactory = createTestHazelcastInstanceFactory(INSTANCE_COUNT);
    }

    protected TestHazelcastInstanceFactory createTestHazelcastInstanceFactory(int count) {
        return createHazelcastInstanceFactory(count);
    }

    protected HazelcastInstance createCacheInstance() {
        return instanceFactory.newHazelcastInstance();
    }

    protected CachingProvider createCachingProvider(HazelcastInstance defaultInstance) {
        return HazelcastServerCachingProvider.createCachingProvider(defaultInstance);
    }

    protected enum InstanceNameUsageType {

        VALID_INSTANCE_NAME,
        INVALID_INSTANCE_NAME,
        NO_INSTANCE_NAME
    }

    protected void checkUsedInstanceInCachingProvider(boolean uriSpecified,
                                                      InstanceNameUsageType instanceNameUsageType) throws URISyntaxException {
        HazelcastInstance instance1 = createCacheInstance();
        HazelcastInstance instance2 = createCacheInstance();
        CachingProvider cachingProvider = createCachingProvider(instance1);
        Properties properties = new Properties();
        if (InstanceNameUsageType.VALID_INSTANCE_NAME == instanceNameUsageType) {
            properties.setProperty(HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME, instance2.getName());
        } else if (InstanceNameUsageType.INVALID_INSTANCE_NAME == instanceNameUsageType) {
            properties.setProperty(HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME, instance2.getName() + "-Invalid");
        }
        HazelcastCacheManager cacheManager =
                (HazelcastCacheManager) cachingProvider.getCacheManager(
                        uriSpecified ? new URI("MyURI") : null, null, properties);

        assertNotNull(cacheManager);

        HazelcastInstance instance = cacheManager.getHazelcastInstance();

        if (InstanceNameUsageType.NO_INSTANCE_NAME == instanceNameUsageType) {
            assertEquals(instance1, instance);
        } else {
            assertEquals(instance2, instance);
        }
    }

    @Test
    public void specifiedInstanceShouldBeUsedWhenInstanceNameIsGivenWithDefaultURI() throws URISyntaxException {
        checkUsedInstanceInCachingProvider(false, InstanceNameUsageType.VALID_INSTANCE_NAME);
    }

    @Test
    public void specifiedInstanceShouldBeUsedWhenInstanceNameIsGivenWithSpecifiedURI() throws URISyntaxException {
        checkUsedInstanceInCachingProvider(true, InstanceNameUsageType.VALID_INSTANCE_NAME);
    }

    @Test(expected = CacheException.class)
    public void cacheManagerShouldNotBeAbleToCreatedWhenGivenInstanceNameIsInvalidWithDefaultURI() throws URISyntaxException {
        checkUsedInstanceInCachingProvider(false, InstanceNameUsageType.INVALID_INSTANCE_NAME);
    }

    @Test(expected = CacheException.class)
    public void cacheManagerShouldNotBeAbleToCreatedWhenGivenInstanceNameIsInvalidWithSpecifiedURI() throws URISyntaxException {
        checkUsedInstanceInCachingProvider(true, InstanceNameUsageType.INVALID_INSTANCE_NAME);
    }

    @Test
    public void defaultInstanceShouldBeUsedWhenInstanceNameIsNotGivenWithDefaultURI() throws URISyntaxException {
        checkUsedInstanceInCachingProvider(false, InstanceNameUsageType.NO_INSTANCE_NAME);
    }

    @Test(expected = CacheException.class)
    public void cacheManagerShouldNotBeAbleToCreatedWhenInstanceNameIsNotGivenWithSpecifiedURI() throws URISyntaxException {
        checkUsedInstanceInCachingProvider(true, InstanceNameUsageType.NO_INSTANCE_NAME);
    }

}
