/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.hibernate.instance;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.HazelcastCacheRegionFactory;
import com.hazelcast.hibernate.provider.HazelcastCacheProvider;
import org.hibernate.cache.CacheProvider;
import org.hibernate.cache.RegionFactory;
import org.hibernate.cache.impl.bridge.RegionFactoryCacheProviderBridge;
import org.hibernate.cfg.Settings;

import java.util.logging.Level;

final class RegionFactoryHazelcastAccessor extends HazelcastAccessor {

    public HazelcastInstance getHazelcastInstance(Settings settings) {
        final RegionFactory rf = settings.getRegionFactory();
        if (rf == null) {
            logger.log(Level.SEVERE, "Hibernate 2nd level cache has not been enabled!");
            return null;
        }
        if (rf instanceof RegionFactoryCacheProviderBridge) {
            final CacheProvider provider = ((RegionFactoryCacheProviderBridge) rf).getCacheProvider();
            if (provider instanceof HazelcastCacheProvider) {
                return ((HazelcastCacheProvider) provider).getHazelcastInstance();
            }
            logger.log(Level.WARNING, "Current 2nd level cache implementation is not HazelcastCacheProvider!");
        } else if (rf instanceof HazelcastCacheRegionFactory) {
            return ((HazelcastCacheRegionFactory) rf).getHazelcastInstance();
        } else {
            logger.log(Level.WARNING, "Current 2nd level cache implementation is not HazelcastCacheRegionFactory!");
        }
        return null;
    }
}
