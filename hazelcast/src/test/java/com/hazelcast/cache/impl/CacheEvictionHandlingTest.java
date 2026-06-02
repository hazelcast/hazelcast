/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.config.Config;

import com.hazelcast.test.LogEntryMatcher;
import com.hazelcast.test.SerialTest;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;

import java.util.function.Predicate;

import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@SerialTest
public class CacheEvictionHandlingTest {
    private static final Logger logger = LoggerFactory.getLogger(CacheEvictionHandlingTest.class);

    private final TestHazelcastInstanceFactory instanceFactory = new TestHazelcastInstanceFactory(1);

    @AfterEach
    public void tearDown() {
        instanceFactory.terminateAll();
        JsrTestUtil.cleanup();
    }

    @Test
    @SetSystemProperty(key = "hazelcast.jcache.provider.type", value = "member")
    void shouldNotThrowIllegalArgumentException_whenAccessTriggersExpirationTimeUpdate() {
        Config hazelcastConfig = smallInstanceConfig();
        hazelcastConfig.setInstanceName(HazelcastCachingProvider.SHARED_JCACHE_INSTANCE_NAME);
        hazelcastConfig.setClusterName("hazelcast-expiration-exception");
        instanceFactory.newHazelcastInstance(hazelcastConfig);

        CacheManager cacheManager = Caching.getCachingProvider().getCacheManager();

        try (var matcher = LogEntryMatcher.install(illegalArgumentExceptionPredicate())) {
            // A cache configuration whose entries expire after access is required to trigger the EXPIRATION_TIME_UPDATED event
            var cacheConfig = new MutableConfiguration<String, Integer>()
                .setTypes(String.class, Integer.class)
                .setStoreByValue(true)
                .setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(SECONDS, 30)))
                .setStatisticsEnabled(false);

            Cache<String, Integer> cache = cacheManager.createCache("cache", cacheConfig);

            // Add EntryListener to enable cache events
            var listenerConfig = new MutableCacheEntryListenerConfiguration<>(
                FactoryBuilder.factoryOf(ExampleCacheEntryListener.class),
                FactoryBuilder.factoryOf(CacheFilter.class),
                false, true);
            cache.registerCacheEntryListener(listenerConfig);

            cache.put("1", 100);
            cache.put("2", 200);

            // was triggering IllegalArgumentException prior to CTT-411
            cache.get("1");
            cache.get("1");
            assertThat(matcher.matched()).isFalse();
        }
    }

    private static Predicate<LogEvent> illegalArgumentExceptionPredicate() {
        return e -> e.getMessage().getFormattedMessage().contains("IllegalArgumentException")
            || e.getThrown() instanceof IllegalArgumentException;
    }

    public static class ExampleCacheEntryListener implements
                                                  CacheEntryCreatedListener<String, Integer> {

        @Override
        public void onCreated(Iterable<CacheEntryEvent<? extends String, ? extends Integer>> cacheEntryEvents)
            throws CacheEntryListenerException {
            logger.info("Entry created: {}", cacheEntryEvents);
        }
    }

    public static class CacheFilter implements CacheEntryEventFilter<String, Integer> {

        @Override
        public boolean evaluate(CacheEntryEvent event) throws CacheEntryListenerException {
            return true;
        }
    }
}
