/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.annotation.QuickTest;
import javax.cache.CachingTest;
import javax.cache.configuration.ConfigurationTest;
import javax.cache.configuration.FactoryBuilderTest;
import javax.cache.configuration.MutableCacheEntryListenerConfigurationTest;
import javax.cache.configuration.MutableConfigurationTest;
import javax.cache.event.CacheEntryListenerExceptionTest;
import javax.cache.expiry.DurationTest;
import javax.cache.expiry.ExpiryPolicyTest;
import javax.cache.integration.CacheLoaderExceptionTest;
import javax.cache.integration.CacheWriterExceptionTest;
import javax.cache.integration.CompletionListenerFutureTest;
import org.jsr107.tck.CacheManagerTest;
import org.jsr107.tck.CacheTest;
import org.jsr107.tck.GetTest;
import org.jsr107.tck.PutTest;
import org.jsr107.tck.RemoveTest;
import org.jsr107.tck.ReplaceTest;
import org.jsr107.tck.StoreByReferenceTest;
import org.jsr107.tck.StoreByValueTest;
import org.jsr107.tck.TypesTest;
import org.jsr107.tck.event.CacheEntryListenerClientServerTest;
import org.jsr107.tck.event.CacheListenerTest;
import org.jsr107.tck.expiry.CacheExpiryTest;
import org.jsr107.tck.integration.CacheLoaderClientServerTest;
import org.jsr107.tck.integration.CacheLoaderTest;
import org.jsr107.tck.integration.CacheLoaderWithExpiryTest;
import org.jsr107.tck.integration.CacheLoaderWithoutReadThroughTest;
import org.jsr107.tck.integration.CacheLoaderWriterTest;
import org.jsr107.tck.integration.CacheWriterClientServerTest;
import org.jsr107.tck.integration.CacheWriterTest;
import org.jsr107.tck.management.CacheMBStatisticsBeanTest;
import org.jsr107.tck.management.CacheMXBeanTest;
import org.jsr107.tck.management.CacheManagerManagementTest;
import org.jsr107.tck.processor.CacheInvokeTest;
import org.jsr107.tck.processor.EntryProcessorExceptionTest;
import org.jsr107.tck.spi.CachingProviderClassLoaderTest;
import org.jsr107.tck.spi.CachingProviderTest;
import org.jsr107.tck.support.ClientServerTest;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


@Category(QuickTest.class)
@RunWith(Suite.class)
@Suite.SuiteClasses({
        ClientServerTest.class, RemoveTest.class, CacheManagerManagementTest.class, CacheMBStatisticsBeanTest.class,
        CacheMXBeanTest.class, PutTest.class, CacheExpiryTest.class, GetTest.class, CacheTest.class, CacheManagerTest.class,
        CachingTest.class, CacheWriterTest.class, CacheLoaderWriterTest.class, CacheLoaderWithExpiryTest.class,
        CacheWriterClientServerTest.class, CacheLoaderTest.class, CacheLoaderWithoutReadThroughTest.class,
        CacheLoaderClientServerTest.class, StoreByValueTest.class, StoreByReferenceTest.class, CachingProviderClassLoaderTest.class,
        CachingProviderTest.class, TypesTest.class, CacheEntryListenerClientServerTest.class, CacheListenerTest.class, ReplaceTest.class,
        CacheInvokeTest.class, EntryProcessorExceptionTest.class, DurationTest.class, ExpiryPolicyTest.class, CachingTest.class,
        CompletionListenerFutureTest.class, CacheWriterExceptionTest.class, CacheLoaderExceptionTest.class, CacheEntryListenerExceptionTest.class,
        ConfigurationTest.class, MutableCacheEntryListenerConfigurationTest.class, FactoryBuilderTest.class, MutableConfigurationTest.class
})

public class TckTests {
    @BeforeClass
    public static void setupProperties() {
//        System.setProperty("javax.management.builder.initial", "com.hazelcast.cache.impl.TCKMBeanServerBuilder");
//        System.setProperty("org.jsr107.tck.management.agentId", "TCKMbeanServer");
//        System.setProperty("javax.cache.Cache.Entry", "com.hazelcast.cache.impl.CacheEntry");
//        System.setProperty("javax.cache.Cache", "com.hazelcast.cache.ICache");
//        System.setProperty("javax.cache.CacheManager", "com.hazelcast.cache.HazelcastCacheManager");
//        System.setProperty("javax.cache.annotation.CacheInvocationContext", "javax.cache.annotation.impl.cdi.CdiCacheKeyInvocationContextImpl");
    }

}
