/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.hibernate.instance;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.provider.HazelcastCacheProvider;
import org.hibernate.cache.CacheProvider;
import org.hibernate.cfg.Settings;

import java.lang.reflect.Method;
import java.util.logging.Level;

final class CacheProviderHazelcastAccessor extends HazelcastAccessor {

    public HazelcastInstance getHazelcastInstance(Settings settings) {
        Object providerObject = null;
        try {
            Method getCacheProviderMethod = Settings.class.getMethod(METHOD_GET_CACHE_PROVIDER);
            providerObject = getCacheProviderMethod.invoke(settings);
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            return null;
        }
        if (providerObject == null) {
            logger.log(Level.SEVERE, "Hibernate 2nd level cache has not been enabled!");
            return null;
        }
        final CacheProvider provider = (CacheProvider) providerObject;
        if (provider instanceof HazelcastCacheProvider) {
            return ((HazelcastCacheProvider) provider).getHazelcastInstance();
        }
        logger.log(Level.WARNING, "Current 2nd level cache implementation is not HazelcastCacheProvider!");
        return null;
    }
}
