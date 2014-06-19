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

package com.hazelcast.hibernate.region;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.RegionCache;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.TransactionalDataRegion;

import java.util.Properties;

/**
 * Abstract base class of all regions
 *
 * @author Leo Kim (lkim@limewire.com)
 * @param <Cache> implementation type of RegionCache
 */
public abstract class AbstractTransactionalDataRegion<Cache extends RegionCache> extends AbstractHazelcastRegion<Cache>
        implements
        TransactionalDataRegion {

    private final CacheDataDescription metadata;
    private final Cache cache;

    protected AbstractTransactionalDataRegion(final HazelcastInstance instance, final String regionName,
                                              final Properties props, final CacheDataDescription metadata, final Cache cache) {
        super(instance, regionName, props);
        this.metadata = metadata;
        this.cache = cache;
    }

    public CacheDataDescription getCacheDataDescription() {
        return metadata;
    }

    public boolean isTransactionAware() {
        return false;
    }

    public Cache getCache() {
        return cache;
    }
}
