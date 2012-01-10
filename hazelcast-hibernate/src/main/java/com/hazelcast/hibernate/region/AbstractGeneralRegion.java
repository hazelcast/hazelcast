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

package com.hazelcast.hibernate.region;

import java.util.Properties;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.GeneralDataRegion;

import com.hazelcast.core.HazelcastInstance;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
public abstract class AbstractGeneralRegion extends AbstractHazelcastRegion implements GeneralDataRegion {

    protected AbstractGeneralRegion(final HazelcastInstance instance, final String name, final Properties properties) {
        super(instance, name, properties);
    }

    public void evict(final Object key) throws CacheException {
        getCache().remove(key);
    }

    public void evictAll() throws CacheException {
        getCache().clear();
    }

    public Object get(final Object key) throws CacheException {
        return getCache().get(key);
    }

    public void put(final Object key, final Object value) throws CacheException {
        getCache().put(key, value);
    }
}
