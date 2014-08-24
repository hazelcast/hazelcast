/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import javax.cache.CacheManager;
import java.net.URI;
import java.util.Properties;

public final class HazelcastServerCachingProvider
        extends HazelcastAbstractCachingProvider {

    public HazelcastServerCachingProvider() {
        super();
    }

    @Override
    protected HazelcastInstance initHazelcast() {
        Config config = new XmlConfigBuilder().build();
        return Hazelcast.newHazelcastInstance(config);
    }

    @Override
    protected CacheManager getHazelcastCacheManager(URI uri, ClassLoader classLoader, Properties managerProperties) {
        return new HazelcastServerCacheManager(this, getHazelcastInstance(), uri, classLoader, managerProperties);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HazelcastServerCachingProvider{");
        sb.append("hazelcastInstance=").append(hazelcastInstance);
        sb.append('}');
        return sb.toString();
    }
}
