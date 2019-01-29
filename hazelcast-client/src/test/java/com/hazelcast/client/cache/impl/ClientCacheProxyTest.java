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

package com.hazelcast.client.cache.impl;

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.logging.Level;

import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientCacheProxyTest extends HazelcastTestSupport {

    private TestLogger logger = new TestLogger();

    @Test
    @SuppressWarnings("deprecation")
    public void isCacheOnUpdate_prints_warning_message_for_deprecated_policy_CACHE() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig().setLocalUpdatePolicy(CACHE);
        NearCachedClientCacheProxy.isCacheOnUpdate(nearCacheConfig, "cacheName", logger);

        assertNotNull(logger.message);
    }

    @Test
    public void isCacheOnUpdate_not_prints_warning_message_for_policy_CACHE_ON_UPDATE() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig().setLocalUpdatePolicy(CACHE_ON_UPDATE);
        NearCachedClientCacheProxy.isCacheOnUpdate(nearCacheConfig, "cacheName", logger);

        assertNull(logger.message);
    }

    private static class TestLogger extends AbstractLogger {

        String message;

        @Override
        public void log(Level level, String message) {
            this.message = message;
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void log(LogEvent logEvent) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Level getLevel() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isLoggable(Level level) {
            throw new UnsupportedOperationException();
        }
    }
}
