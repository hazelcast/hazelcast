/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import classloading.domain.Person;
import classloading.domain.PersonCacheLoaderFactory;
import classloading.domain.PersonEntryProcessor;
import classloading.domain.PersonExpiryPolicyFactory;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.FilteringClassLoader;
import com.hazelcast.util.RootCauseMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.cache.HazelcastCachingProvider.propertiesByInstanceItself;
import static com.hazelcast.config.UserCodeDeploymentConfig.ClassCacheMode.OFF;
import static org.junit.Assert.assertNotNull;

/**
 * Test transfer of CacheConfig's of typed Caches to a joining member
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheTypesConfigTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expect = ExpectedException.none();

    protected String cacheName;
    protected TestHazelcastInstanceFactory factory;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory();
        cacheName = randomName();
    }

    // Even when the key or value class is not present in the classpath of the target member, a joining member will join.
    // Some Cache operations that require deserialization on the member will fail (eg entry processors)
    @Test
    public void cacheConfigShouldBeAddedOnJoiningMember_whenClassNotResolvable() {
        // create a HazelcastInstance with a CacheConfig referring to a Class not resolvable on the joining member
        HazelcastInstance hz1 = factory.newHazelcastInstance(getConfig());
        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(hz1);
        cachingProvider.getCacheManager().createCache(cacheName, createCacheConfig());

        // joining member cannot resolve Person class
        HazelcastInstance hz2 = factory.newHazelcastInstance(getClassFilteringConfig());
        assertClusterSize(2, hz1, hz2);

        ICache<String, Person> cache = hz1.getCacheManager().getCache(cacheName);
        String key = generateKeyOwnedBy(hz2);
        // a simple put will work as hz2 will just receive a Data value blob
        cache.put(key, new Person());

        expect.expectCause(new RootCauseMatcher(ClassNotFoundException.class, "classloading.domain.PersonEntryProcessor - "
                + "Package excluded explicitly"));
        cache.invoke(key, new PersonEntryProcessor());
    }

    // When the joining member is not aware of key or value class but it is later resolvable via user code deployment, then
    // all Cache features should work
    @Test
    public void cacheConfigShouldBeAddedOnJoiningMember_whenKVTypesAvailableViaUserCodeDeployment() {
        // create a HazelcastInstance with a CacheConfig referring to a Class resolvable via user code deployment
        Config config = getConfig();
        UserCodeDeploymentConfig codeDeploymentConfig = new UserCodeDeploymentConfig()
                .setEnabled(true)
                .setClassCacheMode(OFF)
                .setWhitelistedPrefixes("classloading");
        config.setUserCodeDeploymentConfig(codeDeploymentConfig);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(hz1);
        cachingProvider.getCacheManager().createCache(cacheName, createCacheConfig());

        // joining member cannot resolve Person class but it is resolvable on other member via user code deployment
        Config joiningMemberConfig = getClassFilteringConfig();
        joiningMemberConfig.setUserCodeDeploymentConfig(codeDeploymentConfig);
        HazelcastInstance hz2 = factory.newHazelcastInstance(joiningMemberConfig);
        assertClusterSize(2, hz1, hz2);

        ICache<String, Person> testCache = hz1.getCacheManager().getCache(cacheName);
        String key = generateKeyOwnedBy(hz2);
        testCache.put(key, new Person());
        testCache.invoke(key, new PersonEntryProcessor());
        assertNotNull(testCache.get(key));
    }

    // tests deferred resolution of factories
    @Test
    public void cacheConfigShouldBeAddedOnJoiningMember_whenCacheLoaderFactoryNotResolvable() throws InterruptedException {
        HazelcastInstance hz1 = factory.newHazelcastInstance(getConfig());
        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(hz1);
        CacheManager cacheManager1 = cachingProvider.getCacheManager(null, null, propertiesByInstanceItself(hz1));
        CacheConfig cacheConfig = createCacheConfig();
        cacheConfig.setCacheLoaderFactory(new PersonCacheLoaderFactory());
        cacheManager1.createCache(cacheName, cacheConfig);

        // joining member cannot resolve PersonCacheLoaderFactory class
        HazelcastInstance hz2 = factory.newHazelcastInstance(getClassFilteringConfig());
        assertClusterSize(2, hz1, hz2);

        ICache<String, Person> cache = hz2.getCacheManager().getCache(cacheName);
        String key = generateKeyOwnedBy(hz2);
        expect.expectCause(new RootCauseMatcher(ClassNotFoundException.class, "classloading.domain.PersonCacheLoaderFactory - "
                + "Package excluded explicitly"));
        cache.invoke(key, new PersonEntryProcessor());
    }

    // tests deferred resolution of expiry policy factory
    @Test
    public void cacheConfigShouldBeAddedOnJoiningMember_whenExpiryPolicyFactoryNotResolvable() throws InterruptedException {
        HazelcastInstance hz1 = factory.newHazelcastInstance(getConfig());
        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(hz1);
        cachingProvider.getCacheManager().createCache(cacheName, createCacheConfig()
                .setExpiryPolicyFactory(new PersonExpiryPolicyFactory()));

        HazelcastInstance hz2 = factory.newHazelcastInstance(getClassFilteringConfig());
        assertClusterSize(2, hz1, hz2);

        ICache<String, Person> cache = hz2.getCacheManager().getCache(cacheName);
        String key = generateKeyOwnedBy(hz2);
        expect.expectCause(new RootCauseMatcher(ClassNotFoundException.class, "classloading.domain.PersonExpiryPolicyFactory - "
                + "Package excluded explicitly"));
        cache.invoke(key, new PersonEntryProcessor());
    }

    // overridden in another context
    CacheConfig<String, Person> createCacheConfig() {
        CacheConfig<String, Person> cacheConfig = new CacheConfig<String, Person>();
        cacheConfig.setTypes(String.class, Person.class).setManagementEnabled(true);
        return cacheConfig;
    }

    private Config getClassFilteringConfig() {
        List<String> excludes = Arrays.asList("classloading");
        ClassLoader classLoader = new FilteringClassLoader(excludes, null);
        return getConfig().setClassLoader(classLoader);
    }
}
